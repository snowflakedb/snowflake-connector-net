/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Text;
using Mono.Unix;
using Mono.Unix.Native;
using Snowflake.Data.Client;
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

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        private readonly Dictionary<string, string> _tomlToNetPropertiesMapper = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "DATABASE", "DB" }
        };

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;

        internal static readonly TomlConnectionBuilder Instance = new TomlConnectionBuilder();

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
            return connectionToml == null ? string.Empty : GetConnectionStringFromTomlTable(connectionToml);
        }

        private string GetConnectionStringFromTomlTable(TomlTable connectionToml)
        {
            var connectionStringBuilder = new StringBuilder();
            var tokenFilePathValue = string.Empty;
            var isOauth = connectionToml.TryGetValue("authenticator", out var authenticator) && authenticator.ToString().Equals("oauth");
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
            var tokenFile = !string.IsNullOrEmpty(tokenFilePathValue) && _fileOperations.Exists(tokenFilePathValue) ? tokenFilePathValue : DefaultTokenPath;
            s_logger.Debug($"Read token from file path: {tokenFile}");
            return _fileOperations.Exists(tokenFile) ? _fileOperations.ReadAllText(tokenFile, GetFileValidations()) : null;
        }

        private TomlTable GetTomlTableFromConfig(string tomlPath, string connectionName)
        {
            if (!_fileOperations.Exists(tomlPath))
            {
                return null;
            }

            var tomlContent = _fileOperations.ReadAllText(tomlPath, GetFileValidations()) ?? string.Empty;
            var toml = Toml.ToModel(tomlContent);
            if (string.IsNullOrEmpty(connectionName))
            {
                connectionName = _environmentOperations.GetEnvironmentVariable(SnowflakeDefaultConnectionName) ?? DefaultConnectionName;
            }

            var connectionExists = toml.TryGetValue(connectionName, out var connection);
            // In the case where the connection name is the default connection name and does not exist, we will not use the toml builder feature.
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
            tomlPath = Path.GetFullPath(tomlPath);
            return tomlPath;
        }

        internal static Action<UnixStream> GetFileValidations()
        {
            return stream =>
            {
                const FileAccessPermissions forbiddenPermissions = FileAccessPermissions.OtherReadWriteExecute | FileAccessPermissions.GroupReadWriteExecute;
                if (stream.OwnerUser.UserId != Syscall.geteuid())
                    throw new SecurityException("Attempting to read a file not owned by the effective user of the current process");
                if (stream.OwnerGroup.GroupId != Syscall.getegid())
                    throw new SecurityException("Attempting to read a file not owned by the effective group of the current process");
                if ((stream.FileAccessPermissions & forbiddenPermissions) != 0)
                    throw new SecurityException("Attempting to read a file with too broad permissions assigned");
            };
        }
    }
}
