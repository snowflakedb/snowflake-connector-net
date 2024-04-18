/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Mono.Unix.Native;
using Newtonsoft.Json;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using KeyToken = System.Collections.Generic.Dictionary<string, string>;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerIFileImpl : ISnowflakeCredentialManager
    {
        private const string CredentialCacheDirectoryEnvironmentName = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";

        private static readonly string CustomCredentialCacheDirectory = Environment.GetEnvironmentVariable(CredentialCacheDirectoryEnvironmentName);

        private static readonly string DefaultCredentialCacheDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private static readonly string JsonCacheDirectory = string.IsNullOrEmpty(CustomCredentialCacheDirectory) ? DefaultCredentialCacheDirectory : CustomCredentialCacheDirectory;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerIFileImpl>();

        private static readonly string s_jsonPath = Path.Combine(JsonCacheDirectory, "temporary_credential.json");

        private readonly FileOperations _fileOperations;

        private readonly DirectoryOperations _directoryOperations;

        private readonly UnixOperations _unixOperations;

        public static readonly SnowflakeCredentialManagerIFileImpl Instance = new SnowflakeCredentialManagerIFileImpl(FileOperations.Instance, DirectoryOperations.Instance, UnixOperations.Instance);

        internal SnowflakeCredentialManagerIFileImpl(FileOperations fileOperations, DirectoryOperations directoryOperations, UnixOperations unixOperations)
        {
            _fileOperations = fileOperations;
            _directoryOperations = directoryOperations;
            _unixOperations = unixOperations;
        }

        internal void WriteToJsonFile(string content)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _fileOperations.Write(s_jsonPath, content);
                FileInfo info = new FileInfo(s_jsonPath);
                FileSecurity security = info.GetAccessControl();
                FileSystemAccessRule rule = new FileSystemAccessRule(
                    new SecurityIdentifier(WellKnownSidType.CreatorOwnerSid, null),
                    FileSystemRights.FullControl,
                    AccessControlType.Allow);
                security.SetAccessRule(rule);
                info.SetAccessControl(security);
            }
            else
            {
                if (!_directoryOperations.Exists(JsonCacheDirectory))
                {
                    _directoryOperations.CreateDirectory(JsonCacheDirectory);
                }
                var createFileResult = _unixOperations.CreateFileWithPermissions(s_jsonPath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR);
                if (createFileResult == -1)
                {
                    var errorMessage = "Failed to create the JSON token cache file";
                    s_logger.Error(errorMessage);
                    throw new Exception(errorMessage);
                }
                else
                {
                    _fileOperations.Write(s_jsonPath, content);
                }

                var jsonPermissions = _unixOperations.GetFilePermissions(s_jsonPath);
                if (jsonPermissions != FileAccessPermissions.UserReadWriteExecute)
                {
                    var errorMessage = "Permission for the JSON token cache file should contain only the owner access";
                    s_logger.Error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }
        }

        internal KeyToken ReadJsonFile()
        {
            return JsonConvert.DeserializeObject<KeyToken>(File.ReadAllText(s_jsonPath));
        }

        public string GetCredentials(string key)
        {
            if (_fileOperations.Exists(s_jsonPath))
            {
                var keyTokenPairs = ReadJsonFile();

                if (keyTokenPairs.TryGetValue(key, out string token))
                {
                    return token;
                }
            }

            s_logger.Info("Unable to get credentials for the specified key");
            return "";
        }

        public void RemoveCredentials(string key)
        {
            if (_fileOperations.Exists(s_jsonPath))
            {
                var keyTokenPairs = ReadJsonFile();
                keyTokenPairs.Remove(key);
                WriteToJsonFile(JsonConvert.SerializeObject(keyTokenPairs));
            }
        }

        public void SaveCredentials(string key, string token)
        {
            KeyToken keyTokenPairs = _fileOperations.Exists(s_jsonPath) ? ReadJsonFile() : new KeyToken();
            keyTokenPairs[key] = token;

            string jsonString = JsonConvert.SerializeObject(keyTokenPairs);
            WriteToJsonFile(jsonString);
        }
    }
}
