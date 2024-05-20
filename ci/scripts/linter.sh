#!/bin/bash -e
#
# Apply Linter to changed files in PR
set -e
set -o pipefail

current_branch=${GITHUB_HEAD_REF:-${GITHUB_REF#refs/heads/}}
git fetch --no-tags origin master:master
git fetch --no-tags origin $current_branch:$current_branch

BASE_SHA=$(git merge-base $GITHUB_BASE_REF $current_branch)

changed_files=$(git diff --name-only $BASE_SHA HEAD)

echo "All files changed:"
for file in $changed_files; do
    echo "$file"
done

echo "Run dotnet restore"
dotnet restore

echo "Running Dotnet format to changed files"
dotnet format --include $changed_files --verify-no-changes --no-restore
