/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using Mono.Unix;
using Mono.Unix.Native;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingConfigParser>();

        public static readonly EasyLoggingConfigParser Instance = new EasyLoggingConfigParser();

        public virtual ClientConfig Parse(string filePath)
        {
            var configFile = TryToReadFile(filePath);
            return configFile.IsNullOrEmpty() ? null : TryToParseFile(configFile);
        }

        /// <summary>
        /// ReadAllText function reads contents of a file at the <paramref name="filePath"/> making sure, in a way not prone to race-conditions, that:
        ///  - A file is owned by the same user as effective user of the current process.
        ///  - A file is owned by the same group as effective group of the current process.
        ///  - A file permissions do not include `forbiddenPermissions` (any others' permissions by default)
        ///
        /// </summary>
        /// <param name="filePath">The file path of the configuration file</param>
        /// <returns></returns>
        /// <exception cref="SecurityException">An exception will be thrown if the file is no owned by the user or group</exception>
        private string TryToReadFile(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return String.Empty;
            }

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var streamReader = new StreamReader(filePath, Encoding.Default);
                    return streamReader.ReadToEnd();
                }
                else
                {
                    var handle = VerifyUnixPermissions(filePath);

                    var streamReader = new StreamReader(handle, Encoding.Default);
                    return streamReader.ReadToEnd();
                }
            }
            catch (Exception e)
            {
                var errorMessage = "Finding easy logging configuration failed";
                s_logger.Error(errorMessage, e);
                throw;
            }
        }

        private static UnixStream VerifyUnixPermissions(string filePath)
        {
            FileAccessPermissions forbiddenPermissions = FileAccessPermissions.OtherWrite | FileAccessPermissions.GroupWrite;
            var fileInfo = new UnixFileInfo(path: filePath);

            var handle = fileInfo.OpenRead();
            if (handle.OwnerUser.UserId != Syscall.geteuid())
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Attempting to read a file not owned by the effective user of the current process");
            if (handle.OwnerGroup.GroupId != Syscall.getegid())
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Attempting to read a file not owned by the effective group of the current process");
            if ((handle.FileAccessPermissions & forbiddenPermissions) != 0)
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Attempting to read a file with too broad permissions assigned");
            return handle;
        }

        private ClientConfig TryToParseFile(string fileContent)
        {
            try
            {
                var config = JsonConvert.DeserializeObject<ClientConfig>(fileContent);
                Validate(config);
                CheckForUnknownFields(fileContent);
                return config;
            }
            catch (Exception e)
            {
                var errorMessage = "Parsing easy logging configuration failed";
                s_logger.Error(errorMessage, e);
                throw new Exception(errorMessage);
            }
        }

        private void Validate(ClientConfig config)
        {
            if (config.CommonProps.LogLevel != null)
            {
                EasyLoggingLogLevelExtensions.From(config.CommonProps.LogLevel);
            }
        }

        private void CheckForUnknownFields(string fileContent)
        {
            // Parse the specified config file and get the key-value pairs from the "common" section
            List<string> knownProperties = typeof(ClientConfigCommonProps).GetProperties()
                .Select(property => property.GetCustomAttribute<JsonPropertyAttribute>().PropertyName)
                .ToList();

            JObject.Parse(fileContent).GetValue("common", StringComparison.OrdinalIgnoreCase)?
                .Cast<JProperty>()
                .Where(property => !knownProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase))
                .ToList()
                .ForEach(unknownKey => s_logger.Warn($"Unknown field from config: {unknownKey.Name}"));
        }
    }
}
