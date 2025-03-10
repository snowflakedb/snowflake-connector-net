using System;
using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Tomlyn;
using Tomlyn.Model;

namespace Snowflake.Data.Tests.IntegrationTests
{

    [TestFixture, NonParallelizable]
    class SFConnectionWithTomlIT : SFBaseTest
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        private static string s_workingDirectory;


        [SetUp]
        public new void BeforeTest()
        {
            s_workingDirectory ??= Path.Combine(TestContext.CurrentContext.WorkDirectory, "../../..", "toml_config_folder");
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
            CreateTomlConfigBaseOnConnectionString(ConnectionString);
        }

        [TearDown]
        public new void AfterTest()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        public void TestLocalDefaultConnectStringReadFromToml()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome);
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, s_workingDirectory);
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.Open();
                    Assert.AreEqual(ConnectionState.Open, conn.State);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, snowflakeHome);
            }
        }

        [Test]
        public void TestThrowExceptionIfTomlNotFoundWithOtherConnectionString()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome);
            var connectionName = Environment.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName);
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, s_workingDirectory);
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName, "notfoundconnection");
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    Assert.Throws<SnowflakeDbException>(() => conn.Open(), "Unable to connect. Specified connection name does not exist in connections.toml");
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, snowflakeHome);
                Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName, connectionName);
            }
        }

        [Test]
        public void TestThrowExceptionIfTomlFromNotFoundFromDbConnection()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome);
            Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, Path.Combine(s_workingDirectory, "InvalidFolder"));
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    Assert.Throws<SnowflakeDbException>(() => conn.Open(), "Error: Required property ACCOUNT is not provided");
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome, snowflakeHome);
            }
        }

        private static void CreateTomlConfigBaseOnConnectionString(string connectionString)
        {
            var tomlModel = new TomlTable();
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            var defaultTomlTable = new TomlTable();
            tomlModel.Add("default", defaultTomlTable);

            foreach (var property in properties)
            {
                defaultTomlTable.Add(property.Key.ToString(), property.Value);
            }

            var filePath = Path.Combine(s_workingDirectory, "connections.toml");

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var writer = File.CreateText(filePath))
                {
                    writer.Write(Toml.FromModel(tomlModel));
                }
            }
            else
            {
                using (var writer = File.CreateText(filePath))
                {
                    writer.Write(string.Empty);
                }
                Syscall.chmod(filePath, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR);
                using (var writer = File.CreateText(filePath))
                {
                    writer.Write(Toml.FromModel(tomlModel));
                }
                Syscall.chmod(filePath, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR);
            }
        }
    }

}


