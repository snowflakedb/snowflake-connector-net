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
    public class SnowflakeCredentialManagerFileImpl : ISnowflakeCredentialManager
    {
        internal const string CredentialCacheDirectoryEnvironmentName = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";

        internal const string CredentialCacheFileName = "temporary_credential.json";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerFileImpl>();

        private readonly string _jsonCacheDirectory;

        private readonly string _jsonCacheFilePath;

        private readonly FileOperations _fileOperations;

        private readonly DirectoryOperations _directoryOperations;

        private readonly UnixOperations _unixOperations;

        private readonly EnvironmentOperations _environmentOperations;

        public static readonly SnowflakeCredentialManagerFileImpl Instance = new SnowflakeCredentialManagerFileImpl(FileOperations.Instance, DirectoryOperations.Instance, UnixOperations.Instance, EnvironmentOperations.Instance);

        internal SnowflakeCredentialManagerFileImpl(FileOperations fileOperations, DirectoryOperations directoryOperations, UnixOperations unixOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _directoryOperations = directoryOperations;
            _unixOperations = unixOperations;
            _environmentOperations = environmentOperations;
            SetCredentialCachePath(ref _jsonCacheDirectory, ref _jsonCacheFilePath);
        }

        private void SetCredentialCachePath(ref string _jsonCacheDirectory, ref string _jsonCacheFilePath)
        {
            var customDirectory = _environmentOperations.GetEnvironmentVariable(CredentialCacheDirectoryEnvironmentName);
            _jsonCacheDirectory = string.IsNullOrEmpty(customDirectory) ? _environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile) : customDirectory;
            _jsonCacheFilePath = Path.Combine(_jsonCacheDirectory, CredentialCacheFileName);
        }

        internal void WriteToJsonFile(string content)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _fileOperations.Write(_jsonCacheFilePath, content);
                FileInfo info = new FileInfo(_jsonCacheFilePath);
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
                if (!_directoryOperations.Exists(_jsonCacheDirectory))
                {
                    _directoryOperations.CreateDirectory(_jsonCacheDirectory);
                }
                var createFileResult = _unixOperations.CreateFileWithPermissions(_jsonCacheFilePath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR);
                if (createFileResult == -1)
                {
                    var errorMessage = "Failed to create the JSON token cache file";
                    s_logger.Error(errorMessage);
                    throw new Exception(errorMessage);
                }
                else
                {
                    _fileOperations.Write(_jsonCacheFilePath, content);
                }

                var jsonPermissions = _unixOperations.GetFilePermissions(_jsonCacheFilePath);
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
            return JsonConvert.DeserializeObject<KeyToken>(File.ReadAllText(_jsonCacheFilePath));
        }

        public string GetCredentials(string key)
        {
            if (_fileOperations.Exists(_jsonCacheFilePath))
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
            if (_fileOperations.Exists(_jsonCacheFilePath))
            {
                var keyTokenPairs = ReadJsonFile();
                keyTokenPairs.Remove(key);
                WriteToJsonFile(JsonConvert.SerializeObject(keyTokenPairs));
            }
        }

        public void SaveCredentials(string key, string token)
        {
            KeyToken keyTokenPairs = _fileOperations.Exists(_jsonCacheFilePath) ? ReadJsonFile() : new KeyToken();
            keyTokenPairs[key] = token;

            string jsonString = JsonConvert.SerializeObject(keyTokenPairs);
            WriteToJsonFile(jsonString);
        }
    }
}
