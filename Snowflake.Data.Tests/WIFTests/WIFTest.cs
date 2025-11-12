//using System;
//using System.Data;
//using System.Diagnostics;
//using NUnit.Framework;
//using Snowflake.Data.Client;
//using Snowflake.Data.Tests;

//namespace Snowflake.Data.WIFTests
//{
//    /// <summary>
//    /// Running tests locally:
//    /// 1. Push branch to repository
//    /// 2. Set environment variable PARAMETERS_SECRET
//    /// 3. Run ci/test_wif.sh
//    /// </summary>
//    ///
//    [NonParallelizable, IgnoreOnCI]
//    public class WifLatestTest
//    {
//        private static readonly string s_account = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_ACCOUNT");
//        private static readonly string s_host = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_HOST");
//        private static readonly string s_provider = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_WIF_PROVIDER");

//        [Test, IgnoreOnCI]
//        public void TestAuthenticateUsingWifWithDefinedProvider()
//        {
//            var connectionString = $"account={s_account};host={s_host};authenticator=WORKLOAD_IDENTITY;workload_identity_provider={s_provider}";
//            ConnectAndExecuteSimpleQuery(connectionString);
//        }

//        [Test, IgnoreOnCI]
//        public void TestAuthenticateUsingOidc()
//        {
//            if (!IsProviderGcp())
//            {
//                Assert.Ignore("Test only runs when provider is GCP");
//            }

//            var token = GetGcpAccessToken();
//            var connectionString = $"account={s_account};host={s_host};authenticator=WORKLOAD_IDENTITY;workload_identity_provider=OIDC;token={token}";
//            ConnectAndExecuteSimpleQuery(connectionString);
//        }

//        private static bool IsProviderGcp()
//        {
//            return string.Equals(s_provider, "GCP", StringComparison.OrdinalIgnoreCase);
//        }

//        private string GetGcpAccessToken()
//        {
//            try
//            {
//                var startInfo = new ProcessStartInfo
//                {
//                    FileName = "/bin/bash",
//                    Arguments = "-c \"wget --header='Metadata-Flavor: Google' -qO- 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/identity?audience=snowflakecomputing.com'\"",
//                    RedirectStandardOutput = true,
//                    RedirectStandardError = true,
//                    UseShellExecute = false,
//                    CreateNoWindow = true
//                };

//                using var process = Process.Start(startInfo);
//                string token = process.StandardOutput.ReadToEnd();
//                string error = process.StandardError.ReadToEnd();
//                process.WaitForExit();

//                if (process.ExitCode == 0 && !string.IsNullOrWhiteSpace(token))
//                {
//                    return token.Trim();
//                }

//                throw new InvalidOperationException(
//                    $"Failed to retrieve GCP access token, exit code: {process.ExitCode}, error: {error}");
//            }
//            catch (Exception e)
//            {
//                throw new InvalidOperationException("Error executing GCP metadata request", e);
//            }
//        }

//        private void ConnectAndExecuteSimpleQuery(string connectionString)
//        {
//            using (IDbConnection conn = new SnowflakeDbConnection())
//            {
//                conn.ConnectionString = connectionString;
//                conn.Open();
//                Assert.AreEqual(ConnectionState.Open, conn.State);
//                using (IDbCommand command = conn.CreateCommand())
//                {
//                    command.CommandText = "SELECT 1";
//                    var result = command.ExecuteScalar();
//                    Assert.AreEqual("1", result.ToString());
//                }
//            }
//        }
//    }
//}
