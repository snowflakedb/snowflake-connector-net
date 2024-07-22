// <copyright file="SnowflakeTomlConnectionBuilder.cs" company="Snowflake Inc">
//         Copyright (c) 2024 Snowflake Inc. All rights reserved.
//  </copyright>

namespace Snowflake.Data.Core
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Client;
    using Log;
    using Tomlyn;
    using Tomlyn.Model;
    using Tools;

    public class SnowflakeTomlConnectionBuilder
    {
        private const string DefaultConnectionName = "default";
        private const string DefaultSnowflakeFolder = ".snowflake";
        private const string DefaultTokenPath = "/snowflake/session/token";

        private readonly SFLogger _logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        private readonly Dictionary<string, string> _tomlToNetPropertiesMapper = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "DATABASE", "DB" }
        };

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;

        public SnowflakeTomlConnectionBuilder() : this(FileOperations.Instance, EnvironmentOperations.Instance)
        {
        }

        internal SnowflakeTomlConnectionBuilder(FileOperations fileOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _environmentOperations = environmentOperations;
        }

        public string GetConnectionStringFromToml(string connectionName = null)
        {
            var connectionString = string.Empty;
            var tomlPath = ResolveConnectionTomlFile();
            var connectionToml = GetTomlTableFromConfig(tomlPath, connectionName);
            if (connectionToml != null)
            {
                connectionString = GetConnectionStringFromTomlTable(connectionToml);
            }
            return connectionString;
        }

        private string GetConnectionStringFromTomlTable(TomlTable connectionToml)
        {
            var connectionStringBuilder = new StringBuilder();
            var tokenFilePathValue = string.Empty;
            var isOauth = connectionToml.TryGetValue("authenticator", out var authenticator) && authenticator.ToString().Equals("oauth");
            foreach (var property in connectionToml.Keys)
            {
                if (isOauth && property.Equals("token_file_path", StringComparison.InvariantCultureIgnoreCase))
                {
                    tokenFilePathValue = (string)connectionToml[property];
                    continue;
                }
                var mappedProperty = _tomlToNetPropertiesMapper.TryGetValue(property, out var mapped) ? mapped : property;
                connectionStringBuilder.Append($"{mappedProperty}={(string)connectionToml[property]};");
            }

            if (!isOauth || connectionToml.ContainsKey("token"))
                return connectionStringBuilder.ToString();

            var token = LoadTokenFromFile(tokenFilePathValue);
            if (!string.IsNullOrEmpty(token))
            {
                connectionStringBuilder.Append($"token={token};");
            }
            else
            {
                _logger.Warn("The token has empty value");
            }


            return connectionStringBuilder.ToString();
        }

        private string LoadTokenFromFile(string tokenFilePathValue)
        {
            var tokenFile = !string.IsNullOrEmpty(tokenFilePathValue) && _fileOperations.Exists(tokenFilePathValue) ? tokenFilePathValue : DefaultTokenPath;
            _logger.Debug($"Read token from file path: {tokenFile}");
            return _fileOperations.Exists(tokenFile) ? _fileOperations.ReadAllText(tokenFile) : null;
        }

        private TomlTable GetTomlTableFromConfig(string tomlPath, string connectionName)
        {
            if (!_fileOperations.Exists(tomlPath))
            {
                return null;
            }

            var tomlContent = _fileOperations.ReadAllText(tomlPath) ?? string.Empty;
            var toml = Toml.ToModel(tomlContent);
            if (string.IsNullOrEmpty(connectionName))
            {
                connectionName = _environmentOperations.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, DefaultConnectionName);
            }

            var connectionExists = toml.TryGetValue(connectionName, out var connection);
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
            var tomlFolder = _environmentOperations.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, defaultDirectory);
            var tomlPath = Path.Combine(tomlFolder, "connections.toml");
            tomlPath = Path.GetFullPath(tomlPath);
            return tomlPath;
        }
    }
}
