using System;
using System.Data;
using System.Diagnostics;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Tests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.WIFTests
{
    /// <summary>
    /// Running tests locally:
    /// 1. Push branch to repository
    /// 2. Set environment variable PARAMETERS_SECRET
    /// 3. Run ci/test_wif.sh
    /// </summary>
    ///
    public class WifLatestTest
    {
        private static readonly string s_account = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_ACCOUNT");
        private static readonly string s_host = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_HOST");
        private static readonly string s_provider = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_PROVIDER");
        private static readonly string s_impersonationPath = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH");
        private static readonly string s_expectedUsername = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_USERNAME");
        private static readonly string s_expectedUsernameImpersonation = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION");

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingWifWithDefinedProvider()
        {
            var connectionString = $"account={s_account};host={s_host};authenticator=WORKLOAD_IDENTITY;workload_identity_provider={s_provider};certRevocationCheckMode=enabled;";
            var user = ConnectAndQueryCurrentUser(connectionString);
            if (!string.IsNullOrEmpty(s_expectedUsername))
            {
                Assert.Equal(s_expectedUsername, user);
            }
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingWifWithImpersonation()
        {
            if (string.IsNullOrEmpty(s_impersonationPath))
            {
                Skip.When(true, "Test only runs when SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH is set");
            }

            // connect with impersonation
            var connectionString = $"account={s_account};host={s_host};authenticator=WORKLOAD_IDENTITY;workload_identity_provider={s_provider};workload_impersonation_path={s_impersonationPath};certRevocationCheckMode=enabled;";
            var impersonatedUser = ConnectAndQueryCurrentUser(connectionString);

            // verify the impersonated user matches the expected username
            if (!string.IsNullOrEmpty(s_expectedUsernameImpersonation))
            {
                Assert.Equal(s_expectedUsernameImpersonation, impersonatedUser);
            }

            // verify that impersonation resulted in a different user than the direct identity
            if (!string.IsNullOrEmpty(s_expectedUsername))
            {
                Assert.NotEqual(s_expectedUsername, impersonatedUser);
            }
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOidc()
        {
            if (!IsProviderGcp())
            {
                Skip.When(true, "Test only runs when provider is GCP");
            }

            var token = GetGcpAccessToken();
            var connectionString = $"account={s_account};host={s_host};authenticator=WORKLOAD_IDENTITY;workload_identity_provider=OIDC;token={token};certRevocationCheckMode=enabled;";
            ConnectAndExecuteSimpleQuery(connectionString);
        }

        private static bool IsProviderGcp()
        {
            return string.Equals(s_provider, "GCP");
        }

        private string GetGcpAccessToken()
        {
            try
            {
                var startInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = "-c \"wget --header='Metadata-Flavor: Google' -qO- 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/identity?audience=snowflakecomputing.com'\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using var process = Process.Start(startInfo);
                string token = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                process.WaitForExit();

                if (process.ExitCode == 0 && !string.IsNullOrWhiteSpace(token))
                {
                    return token.Trim();
                }

                throw new InvalidOperationException(
                    $"Failed to retrieve GCP access token, exit code: {process.ExitCode}, error: {error}");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Error executing GCP metadata request", e);
            }
        }

        private string ConnectAndQueryCurrentUser(string connectionString)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    var result = command.ExecuteScalar();
                    Assert.NotNull(result);
                    var user = result.ToString();
                    Assert.NotEmpty(user);
                    return user;
                }
            }
        }

        private void ConnectAndExecuteSimpleQuery(string connectionString)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT 1";
                    var result = command.ExecuteScalar();
                    Assert.Equal("1", result.ToString());
                }
            }
        }
    }
}
