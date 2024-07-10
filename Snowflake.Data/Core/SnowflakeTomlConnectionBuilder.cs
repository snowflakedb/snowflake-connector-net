// <copyright file="SnowflakeTomlConnectionBuilder.cs" company="Snowflake Inc">
//         Copyright (c) 2024 Snowflake Inc. All rights reserved.
//  </copyright>

namespace Snowflake.Data.Core
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Tomlyn;
    using Tomlyn.Model;
    using Tools;

    public class SnowflakeTomlConnectionBuilder
    {
        private const string DefaultConnectionName = "default";
        private const string DefaultSnowflakeHomeDirectory = "~/.snowflake";

        private Dictionary<string, string> TomlToNetPropertiesMapper = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
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
            var tomlPath = ResolveConnectionTomlFile();
            var connectionToml = GetTomlTableFromConfig(tomlPath, connectionName);
            if (connectionToml != null)
            {
                var connectionString = GetConnectionStringFromTomlTable(connectionToml);
                return connectionString;
            }
            return string.Empty;
        }

        private string GetConnectionStringFromTomlTable(TomlTable connectionToml)
        {
            var connectionStringBuilder = new StringBuilder();
            foreach (var property in connectionToml.Keys)
            {
                var mappedProperty = TomlToNetPropertiesMapper.TryGetValue(property, out var mapped) ? mapped : property;
                connectionStringBuilder.Append($"{mappedProperty}={(string)connectionToml[property]};");
            }

            return connectionStringBuilder.ToString();
        }

        private TomlTable GetTomlTableFromConfig(string tomlPath, string connectionName)
        {
            TomlTable result = null;
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

            result = connection as TomlTable;
            return result;
        }

        private string ResolveConnectionTomlFile()
        {
            var tomlFolder = _environmentOperations.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, DefaultSnowflakeHomeDirectory);
            var homeDirectory = HomeDirectoryProvider.HomeDirectory(_environmentOperations);
            tomlFolder = tomlFolder.Replace("~/", $"{homeDirectory}/");
            var tomlPath = Path.Combine(tomlFolder, "connections.toml");
            tomlPath = Path.GetFullPath(tomlPath);
            return tomlPath;
        }
    }
}
