using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using Mono.Unix;
using Mono.Unix.Native;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Tomlyn;
using Tomlyn.Model;

namespace Snowflake.Data.Core
{
    internal class TomlConnectionBuilder
    {
        private const string DefaultConnectionName = "default";
        private const string DefaultSnowflakeFolder = ".snowflake";
        private const string DefaultTokenPath = "/snowflake/session/token";

        internal const string SnowflakeDefaultConnectionName = "SNOWFLAKE_DEFAULT_CONNECTION_NAME";
        internal const string SnowflakeHome = "SNOWFLAKE_HOME";
        internal const string SkipWarningForReadPermissions = "SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        private readonly Dictionary<string, string> _tomlToNetPropertiesMapper = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "DATABASE", "DB" }
        };

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;

        private static bool _skipWarningForReadPermissions = false;

        public static readonly TomlConnectionBuilder Instance = new TomlConnectionBuilder();

        private TomlConnectionBuilder() : this(FileOperations.Instance, EnvironmentOperations.Instance)
        {
        }

        internal TomlConnectionBuilder(FileOperations fileOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _environmentOperations = environmentOperations;
        }

        public string GetConnectionStringFromToml(string connectionName = null)
        {
            var tomlPath = ResolveConnectionTomlFile();
            var connectionToml = GetTomlTableFromConfig(tomlPath, connectionName);
            s_logger.Info($"Reading connection parameters from file using key: {connectionName} and path: {tomlPath}");
            return connectionToml == null ? string.Empty : GetConnectionStringFromTomlTable(connectionToml);
        }

        private string GetConnectionStringFromTomlTable(TomlTable connectionToml)
        {
            var connectionStringBuilder = new StringBuilder();
            var tokenFilePathValue = string.Empty;
            var isOauth = connectionToml.TryGetValue("authenticator", out var authenticator) && OAuthAuthenticator.IsOAuthAuthenticator(authenticator.ToString());
            foreach (var property in connectionToml.Keys)
            {
                var propertyValue = (string)connectionToml[property];
                if (isOauth && property.Equals("token_file_path", StringComparison.InvariantCultureIgnoreCase))
                {
                    tokenFilePathValue = propertyValue;
                    continue;
                }
                var mappedProperty = _tomlToNetPropertiesMapper.TryGetValue(property, out var mapped) ? mapped : property;
                connectionStringBuilder.Append($"{mappedProperty}={propertyValue};");
            }

            AppendTokenFromFileIfNotGivenExplicitly(connectionToml, isOauth, connectionStringBuilder, tokenFilePathValue);
            return connectionStringBuilder.ToString();
        }

        private void AppendTokenFromFileIfNotGivenExplicitly(TomlTable connectionToml, bool isOauth,
            StringBuilder connectionStringBuilder, string tokenFilePathValue)
        {
            if (!isOauth || connectionToml.ContainsKey("token"))
            {
                return;
            }

            s_logger.Info($"Trying to load token from file {tokenFilePathValue}");
            var token = LoadTokenFromFile(tokenFilePathValue);
            if (!string.IsNullOrEmpty(token))
            {
                connectionStringBuilder.Append($"token={token};");
            }
            else
            {
                s_logger.Warn("The token has empty value");
            }
        }

        private string LoadTokenFromFile(string tokenFilePathValue)
        {
            string tokenFile;
            if (string.IsNullOrEmpty(tokenFilePathValue))
            {
                tokenFile = DefaultTokenPath;
            }
            else
            {
                if (!_fileOperations.Exists(tokenFilePathValue))
                {
                    s_logger.Info($"Specified token file {tokenFilePathValue} does not exists.");
                    throw new SnowflakeDbException(SFError.INVALID_CONNECTION_PARAMETER_VALUE, tokenFilePathValue, "token_file_path");
                }

                tokenFile = tokenFilePathValue;
            }
            s_logger.Info($"Read token from file path: {tokenFile}");
            return _fileOperations.Exists(tokenFile) ? _fileOperations.ReadAllText(tokenFile, ValidateFilePermissions) : null;
        }

        private TomlTable GetTomlTableFromConfig(string tomlPath, string connectionName)
        {
            if (!_fileOperations.Exists(tomlPath))
            {
                return null;
            }

            var tomlContent = _fileOperations.ReadAllText(tomlPath, ValidateFilePermissions) ?? string.Empty;
            var toml = Toml.ToModel(tomlContent);
            if (string.IsNullOrEmpty(connectionName))
            {
                connectionName = _environmentOperations.GetEnvironmentVariable(SnowflakeDefaultConnectionName) ?? DefaultConnectionName;
            }

            var connectionExists = toml.TryGetValue(connectionName, out var connection);
            // Avoid handling error when default connection does not exist, user could not want to use toml configuration and forgot to provide the
            // connection string, this error should be thrown later when the undefined connection string is used.
            if (!connectionExists && connectionName != DefaultConnectionName)
            {
                throw new Exception("Specified connection name does not exist in connections.toml");
            }

            var result = connection as TomlTable;
            return result;
        }

        private string ResolveConnectionTomlFile()
        {
            var defaultDirectory = Path.Combine(HomeDirectoryProvider.HomeDirectory(_environmentOperations), DefaultSnowflakeFolder);
            var tomlFolder = _environmentOperations.GetEnvironmentVariable(SnowflakeHome) ?? defaultDirectory;
            var tomlPath = Path.Combine(tomlFolder, "connections.toml");
            return tomlPath;
        }


        internal static void ValidateFilePermissions(UnixStream stream)
        {
            var allowedPermissions = new[]
            {
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupRead | FileAccessPermissions.OtherRead,
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.OtherRead,
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupRead,
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite,
                FileAccessPermissions.UserRead
            };
            if (stream.OwnerUser.UserId != Syscall.geteuid())
                throw new SecurityException("Attempting to read a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != Syscall.getegid())
                throw new SecurityException("Attempting to read a file not owned by the effective group of the current process");

            bool.TryParse(EnvironmentOperations.Instance.GetEnvironmentVariable(SkipWarningForReadPermissions), out _skipWarningForReadPermissions);
            if (!_skipWarningForReadPermissions)
            {
                var nonUserReadPermissions = new[]
                {
                    FileAccessPermissions.GroupRead,
                    FileAccessPermissions.OtherRead
                };
                if (nonUserReadPermissions.Any(p => (stream.FileAccessPermissions & p) == p))
                    s_logger.Warn("File is readable by someone other than the owner");
            }

            if (!(allowedPermissions.Any(a => stream.FileAccessPermissions == a)))
                throw new SecurityException("Attempting to read a file with too broad permissions assigned");
        }
    }
}
