using System;
using System.Threading;
using System.Diagnostics;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.AuthenticationTests
{
    public class AuthTestHelper
    {
        private Exception _exception;
        private readonly bool runAuthTestsManually;
        public AuthTestHelper()
        {
            string envVar = Environment.GetEnvironmentVariable("RUN_AUTH_TESTS_MANUALLY");
            runAuthTestsManually = string.IsNullOrEmpty(envVar) ? true : bool.Parse(envVar);
        }

        public void ProvideCredentials(string scenario, string login, string password)
        {
            try
            {
                String provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js";

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

                    Console.WriteLine("Output: " + output);
                    Console.WriteLine("Error: " + error);
                }
            }
            catch (Exception e)
            {
                _exception = e;
            }
        }

        public void cleanBrowserProcess()
        {
            if (!runAuthTestsManually)
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
                        if (!process.WaitForExit(20000)) // Wait for 15 seconds
                        {
                            process.Kill();
                            throw new TimeoutException("The process did not complete in the allotted time.");
                        }
                        string output = process.StandardOutput.ReadToEnd();
                        string error = process.StandardError.ReadToEnd();

                        Console.WriteLine("Output: " + output);
                        Console.WriteLine("Error: " + error);
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
                        Console.WriteLine(result.ToString());
                        Assert.AreEqual("1", result.ToString());
                    }
                }
            }
            catch (SnowflakeDbException e)
            {
                _exception = e;
            }
        }

        public Thread getConnectAndExecuteSimpleQueryThread(string parameters)
        {
            return new Thread(() => ConnectAndExecuteSimpleQuery(parameters));
        }

        public Thread getProvideCredentialsThread(string scenario, string login, string password)
        {
            return new Thread(() => ProvideCredentials(scenario, login, password));
        }

        public void verifyExceptionIsNotThrown() {
            Assert.That(_exception, Is.Null, "Unexpected exception thrown");
        }

        public void verifyExceptionIsThrown(string error) {
            Assert.That(_exception, Is.Not.Null, "Expected exception was not thrown");
            Assert.That(_exception.Message, Does.Contain(error), "Unexpected exception message.");

        }

        public void connectAndProvideCredentials(Thread provideCredentialsThread, Thread connectThread)
        {
            if (runAuthTestsManually)
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
    }
}

