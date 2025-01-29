using System;
using System.Threading;
using System.Diagnostics;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Snowflake.Data.Tests;


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

        private void ProvideCredentials(string scenario, string login, string password)
        {
            try
            {
                string provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js";

                var startInfo = new ProcessStartInfo
                {
                    FileName = "node",
                    Arguments = provideBrowserCredentialsPath + " " + scenario + " " + login + " " + password,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                using (var process = new Process { StartInfo = startInfo })
                {
                    process.Start();
                    if (!process.WaitForExit(15000)) // Wait for 15 seconds
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
            catch (Exception e)
            {
                _exception = e;
            }
        }

        public void CleanBrowserProcess()
        {
            if (_runAuthTestsManually)
                return;
            {
                try {
                    string cleanBrowserProcessesPath = "/externalbrowser/cleanBrowserProcesses.js";
                    var startInfo = new ProcessStartInfo
                    {
                        FileName = "node",
                        Arguments = cleanBrowserProcessesPath,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };
                    using (var process = new Process { StartInfo = startInfo })
                    {
                        process.Start();
                        if (!process.WaitForExit(20000)) // Wait for 20 seconds
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
                catch (Exception e)
                {
                    throw new Exception(e.ToString());
                }
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

        public Thread GetConnectAndExecuteSimpleQueryThread(string parameters)
        {
            return new Thread(() => ConnectAndExecuteSimpleQuery(parameters));
        }

        public Thread GetProvideCredentialsThread(string scenario, string login, string password)
        {
            return new Thread(() => ProvideCredentials(scenario, login, password));
        }

        public void VerifyExceptionIsNotThrown() {
            Assert.That(_exception, Is.Null, "Unexpected exception thrown");
        }

        public void VerifyExceptionIsThrown(string error) {
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

        public class IgnoreOnCI : IgnoreOnEnvIsAttribute
        {
            public IgnoreOnCI(string reason = null) : base("CI", new[] { "true" }, reason)
            {
            }
        }
    }
}

