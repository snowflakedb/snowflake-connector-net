#!/bin/bash -e
#
# WIF e2e test orchestrator. Runs on the Jenkins node.
#
# Strategy: the test binaries are prebuilt on the Jenkins node by
# ci/build_wif_artifacts.sh and this script ships them to each WIF VM
# and runs them there inside a Docker container. The container inherits
# the VM's cloud identity via IMDS, which is what the WIF flow attests
# against.
#
# Prerequisites (run before this script):
#   * ci/build_wif_artifacts.sh has populated ci/wif/artifacts/
#   * PARAMETERS_SECRET is exported (GPG passphrase for the encrypted params)
#
set -o pipefail

export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export RSA_KEY_PATH_AWS_AZURE="$THIS_DIR/wif/parameters/rsa_wif_aws_azure"
export RSA_KEY_PATH_GCP="$THIS_DIR/wif/parameters/rsa_wif_gcp"
export PARAMETERS_FILE_PATH="$THIS_DIR/wif/parameters/parameters_wif.json"
export ARTIFACT_DIR="$THIS_DIR/wif/artifacts"

RUNTIME_IMAGE="snowflakedb/client-dotnet-ubuntu204-net9-test:2"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

run_wif_tests() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"
  local rsa_key_path="$4"

  local impersonation_path_var="SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_${provider}"
  local username_var="SNOWFLAKE_TEST_WIF_USERNAME_${provider}"
  local username_impersonation_var="SNOWFLAKE_TEST_WIF_USERNAME_${provider}_IMPERSONATION"

  local remote_dir="wif_${provider}_${TIMESTAMP}"
  local ssh_opts=(-i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -p 443 -o UserKnownHostsFile=/dev/null)
  local scp_opts=(-P 443 -i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

  echo "==================================================================="
  echo "WIF tests: ${provider}  (host=${host}, remote_dir=${remote_dir})"
  echo "==================================================================="

  # Create remote directory and upload artifact
  ssh "${ssh_opts[@]}" "$host" "mkdir -p \"$remote_dir\"" || {
    echo "ERROR: failed to create remote dir '$remote_dir' on $host" >&2
    return 1
  }

  scp "${scp_opts[@]}" "$ARTIFACT_DIR/wif_tests.tar.gz" "$host:$remote_dir/wif_tests.tar.gz" || {
    echo "ERROR: failed to scp artifact to $host:$remote_dir/" >&2
    return 1
  }

  # Extract and run tests on the VM inside a Docker container
  ssh "${ssh_opts[@]}" "$host" \
    env REMOTE_DIR="$remote_dir" \
        RUNTIME_IMAGE="$RUNTIME_IMAGE" \
        SNOWFLAKE_TEST_WIF_PROVIDER="$provider" \
        SNOWFLAKE_TEST_WIF_HOST="$snowflake_host" \
        SNOWFLAKE_TEST_WIF_ACCOUNT="$SNOWFLAKE_TEST_WIF_ACCOUNT" \
        SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH="${!impersonation_path_var}" \
        SNOWFLAKE_TEST_WIF_USERNAME="${!username_var}" \
        SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION="${!username_impersonation_var}" \
        bash <<'EOF'
    set -e
    set -o pipefail
    mkdir -p "$HOME/$REMOTE_DIR/tests"
    tar xzf "$HOME/$REMOTE_DIR/wif_tests.tar.gz" -C "$HOME/$REMOTE_DIR/tests"
    docker run \
      --rm \
      --cpus=1 \
      -m 2g \
      --entrypoint "" \
      -v "$HOME/$REMOTE_DIR/tests":/tests \
      -e SNOWFLAKE_TEST_WIF_PROVIDER \
      -e SNOWFLAKE_TEST_WIF_HOST \
      -e SNOWFLAKE_TEST_WIF_ACCOUNT \
      -e SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH \
      -e SNOWFLAKE_TEST_WIF_USERNAME \
      -e SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION \
      "$RUNTIME_IMAGE" \
      dotnet /tests/Snowflake.Data.Tests.dll -namespace "Snowflake.Data.WIFTests"
EOF
}

run_tests_and_set_result() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"
  local rsa_key_path="$4"

  run_wif_tests "$provider" "$host" "$snowflake_host" "$rsa_key_path"
  local status=$?

  if [[ $status -ne 0 ]]; then
    echo "$provider tests failed with exit status: $status"
    EXIT_STATUS=1
  else
    echo "$provider tests passed"
  fi

  # Clean up remote directory
  ssh -i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 443 "$host" \
    "rm -rf \"wif_${provider}_${TIMESTAMP}\"" || true
}

setup_parameters() {
  source "$THIS_DIR/scripts/setup_gpg.sh"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_AWS_AZURE" "${RSA_KEY_PATH_AWS_AZURE}.gpg"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_GCP" "${RSA_KEY_PATH_GCP}.gpg"
  chmod 600 "$RSA_KEY_PATH_AWS_AZURE"
  chmod 600 "$RSA_KEY_PATH_GCP"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$PARAMETERS_FILE_PATH" "${PARAMETERS_FILE_PATH}.gpg"
  eval $(jq -r '.wif | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETERS_FILE_PATH)
}

# Verify artifacts exist
if [[ ! -f "$ARTIFACT_DIR/wif_tests.tar.gz" ]]; then
  echo "ERROR: $ARTIFACT_DIR/wif_tests.tar.gz not found. Run ci/build_wif_artifacts.sh first." >&2
  exit 1
fi

setup_parameters

# Run tests for all cloud providers
EXIT_STATUS=0
set +e  # Don't exit on first failure
run_tests_and_set_result "AZURE" "$HOST_AZURE" "$SNOWFLAKE_TEST_WIF_HOST_AZURE" "$RSA_KEY_PATH_AWS_AZURE"
run_tests_and_set_result "AWS"   "$HOST_AWS"   "$SNOWFLAKE_TEST_WIF_HOST_AWS"   "$RSA_KEY_PATH_AWS_AZURE"
run_tests_and_set_result "GCP"   "$HOST_GCP"   "$SNOWFLAKE_TEST_WIF_HOST_GCP"   "$RSA_KEY_PATH_GCP"
set -e  # Re-enable exit on error
echo "Exit status: $EXIT_STATUS"
exit $EXIT_STATUS
