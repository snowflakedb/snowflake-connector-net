using System;
using System.Threading;
using System.Diagnostics;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Log;

namespace Snowflake.Data.AuthenticationTests
{

    public class AuthTestHelper
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<AuthTestHelper>();
        private Exception _exception;
        private readonly bool _runAuthTestsManually;

        public AuthTestHelper()
        {
            string envVar = Environment.GetEnvironmentVariable("RUN_AUTH_TESTS_MANUALLY");
            _runAuthTestsManually = bool.Parse(envVar ?? "true");
        }

        public void CleanBrowserProcess()
        {
            if (_runAuthTestsManually)
                return;
            try
            {
                StartNodeProcess("/externalbrowser/cleanBrowserProcesses.js", TimeSpan.FromSeconds(20));
            }
            catch (Exception e)
            {
                throw new Exception(e.ToString());
            }
        }

        public void ConnectAndExecuteSimpleQuery(string connectionString)
        {
            try
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = connectionString;

                    conn.Open();
                    Assert.AreEqual(ConnectionState.Open, conn.State);

                    using (IDbCommand command = conn.CreateCommand())
                    {
                        command.CommandText = "SELECT 1";
                        var result = command.ExecuteScalar();
                        Assert.AreEqual("1", result.ToString());
                        s_logger.Info(result.ToString());
                    }
                }
            }
            catch (SnowflakeDbException e)
            {
                _exception = e;
            }
        }

        public bool ConnectAndExecuteSimpleQueryWithMfaToken(string baseConnectionString, string[] totpCodes)
        {
            // Try each TOTP code until one works
            for (int i = 0; i < totpCodes.Length; i++)
            {
                var totpCode = totpCodes[i];
                s_logger.Info($"Trying TOTP code {i + 1}/{totpCodes.Length}");

                // Clear previous exception
                _exception = null;

                // Add passcode to connection string
                var connectionStringWithPasscode = baseConnectionString;
                if (!string.IsNullOrEmpty(totpCode))
                {
                    connectionStringWithPasscode += $";PASSCODE={totpCode}";
                }

                // Attempt connection
                ConnectAndExecuteSimpleQuery(connectionStringWithPasscode);

                if (_exception == null)
                {
                    s_logger.Info($"Successfully connected with TOTP code {i + 1}");
                    return true;
                }
                else
                {
                    var lastError = _exception.Message;
                    s_logger.Warn($"TOTP code {i + 1} failed: {lastError}");

                    if (lastError.Contains("TOTP Invalid") || lastError.Contains("Invalid MFA"))
                    {
                        s_logger.Info("TOTP/MFA error detected.");
                    }
                    else
                    {
                        s_logger.Error($"Non-TOTP error detected: {lastError}");
                        break;
                    }
                }
            }

            s_logger.Error("All TOTP codes failed or non-recoverable error occurred");
            return false;
        }

        public string ConnectUsingOktaConnectionAndExecuteCustomCommand(string command, bool returnToken = false)
        {
            try
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    var parameters = AuthConnectionString.GetOktaConnectionString();
                    conn.ConnectionString = AuthConnectionString.ConvertToConnectionString(parameters);
                    conn.Open();
                    Assert.AreEqual(ConnectionState.Open, conn.State);
                    using (IDbCommand dbCommand = conn.CreateCommand())
                    {
                        dbCommand.CommandText = command;
                        using (var reader = dbCommand.ExecuteReader())
                        {
                            if (returnToken && reader.Read())
                            {
                                return reader["token_secret"].ToString();
                            }
                        }
                    }
                }
            }
            catch (SnowflakeDbException e)
            {
                _exception = e;
            }
            return null;
        }

        public Thread GetConnectAndExecuteSimpleQueryThread(string parameters)
        {
            return new Thread(() => ConnectAndExecuteSimpleQuery(parameters));
        }

        public Thread GetProvideCredentialsThread(string scenario, string login, string password)
        {
            return new Thread(() => ProvideCredentials(scenario, login, password));
        }

        public void VerifyExceptionIsNotThrown()
        {
            Assert.That(_exception, Is.Null, "Unexpected exception thrown");
        }

        public void VerifyExceptionIsThrown(string error)
        {
            Assert.That(_exception, Is.Not.Null, "Expected exception was not thrown");
            Assert.That(_exception.Message, Does.Contain(error), "Unexpected exception message.");

        }

        public void ConnectAndProvideCredentials(Thread provideCredentialsThread, Thread connectThread)
        {
            if (_runAuthTestsManually)
            {
                connectThread.Start();
                connectThread.Join();
            }
            else
            {
                provideCredentialsThread.Start();
                connectThread.Start();
                provideCredentialsThread.Join();
                connectThread.Join();
            }
        }

        private void StartNodeProcess(string path, TimeSpan timeout)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "node",
                Arguments = path,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using (var process = new Process { StartInfo = startInfo })
            {
                process.Start();
                if (!process.WaitForExit((int)timeout.TotalMilliseconds))
                {
                    process.Kill();
                    throw new TimeoutException("The process did not complete in the allotted time.");
                }
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();

                s_logger.Info("Output: " + output);
                s_logger.Info("Error: " + error);
            }
        }

        internal void RemoveTokenFromCache(string tokenHost, string user, TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(tokenHost, user, tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(cacheKey);
        }

        public string[] GetTotp(string seed = "")
        {
            if (_runAuthTestsManually)
            {
                s_logger.Info("TOTP generation is not supported in manual test mode");
                return Array.Empty<string>();
            }
            else
            {
                try
                {
                    string provideTotpGeneratorPath = "/externalbrowser/totpGenerator.js";
                    var startInfo = new ProcessStartInfo
                    {
                        FileName = "node",
                        Arguments = $"{provideTotpGeneratorPath} {seed}",
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };

                    using (var process = new Process { StartInfo = startInfo })
                    {
                        process.Start();
                        if (!process.WaitForExit(40000))
                        {
                            process.Kill();
                            throw new TimeoutException("TOTP generation process did not complete in the allotted time.");
                        }

                        string output = process.StandardOutput.ReadToEnd().Trim();
                        string error = process.StandardError.ReadToEnd();

                        s_logger.Debug($"OUTPUT: {output}, ERRORS: {error}");

                        if (process.ExitCode != 0)
                        {
                            throw new Exception($"TOTP generation failed with exit code {process.ExitCode}: {error}");
                        }

                        return output.Split(Array.Empty<char>(), StringSplitOptions.RemoveEmptyEntries);
                    }
                }
                catch (Exception e)
                {
                    _exception = e;
                    throw new Exception(e.Message, e);
                }
            }
        }

        private void ProvideCredentials(string scenario, string login, string password)
        {
            try
            {
                StartNodeProcess($"/externalbrowser/provideBrowserCredentials.js {scenario} {login} {password}", TimeSpan.FromSeconds(25));
            }
            catch (Exception e)
            {
                _exception = e;
            }
        }
    }
}
