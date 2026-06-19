using System;
using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;
using Tomlyn;
using Tomlyn.Model;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [Collection(nameof(SFConnectionWithTomlIT))]
    public sealed class SFConnectionWithTomlIT : SFBaseTestAsync, IDisposable
    {
        [CollectionDefinition(nameof(SFConnectionWithTomlIT), DisableParallelization = true)]
        public sealed class SFConnectionWithTomlITFixture : ICollectionFixture<SFConnectionWithTomlITFixture>
        {
        }

        private readonly SFBaseTestAsyncFixture _fixture;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionIT>();
        private static string s_workingDirectory;

        public SFConnectionWithTomlIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            s_workingDirectory ??= Path.Combine(AppContext.BaseDirectory, "../../..", "toml_config_folder");
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
            CreateTomlConfigBaseOnConnectionString(_fixture.ConnectionString);
        }

        public void Dispose()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [SFFact]
        public async Task TestLocalDefaultConnectStringReadFromToml()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(EnvVars.SnowflakeHome.Name);
            Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, s_workingDirectory);
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    await conn.OpenAsync();
                    Assert.Equal(ConnectionState.Open, conn.State);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, snowflakeHome);
            }
        }

        [SFFact]
        public async Task TestThrowExceptionIfTomlNotFoundWithOtherConnectionString()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(EnvVars.SnowflakeHome.Name);
            var connectionName = Environment.GetEnvironmentVariable(EnvVars.DefaultConnectionName.Name);
            Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, s_workingDirectory);
            Environment.SetEnvironmentVariable(EnvVars.DefaultConnectionName.Name, "notfoundconnection");
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    await Assert.ThrowsAsync<Exception>(() => conn.OpenAsync()).ConfigureAwait(false);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, snowflakeHome);
                Environment.SetEnvironmentVariable(EnvVars.DefaultConnectionName.Name, connectionName);
            }
        }

        [SFFact]
        public async Task TestThrowExceptionIfTomlFromNotFoundFromDbConnection()
        {
            var snowflakeHome = Environment.GetEnvironmentVariable(EnvVars.SnowflakeHome.Name);
            Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, Path.Combine(s_workingDirectory, "InvalidFolder"));
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync()).ConfigureAwait(false);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(EnvVars.SnowflakeHome.Name, snowflakeHome);
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


