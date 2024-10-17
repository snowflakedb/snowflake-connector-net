/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Mono.Unix.Native;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Runtime.InteropServices;
using KeyTokenDict = System.Collections.Generic.Dictionary<string, string>;

namespace Snowflake.Data.Core
{
    public class SnowflakeCredentialManagerFileImpl : ISnowflakeCredentialManager
    {
        internal const string CredentialCacheDirectoryEnvironmentName = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";

        internal const string CredentialCacheDirName = ".snowflake";

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
            _jsonCacheDirectory = string.IsNullOrEmpty(customDirectory) ? Path.Combine(HomeDirectoryProvider.HomeDirectory(_environmentOperations), CredentialCacheDirName) : customDirectory;
            if (!_directoryOperations.Exists(_jsonCacheDirectory))
            {
                _directoryOperations.CreateDirectory(_jsonCacheDirectory);
            }
            _jsonCacheFilePath = Path.Combine(_jsonCacheDirectory, CredentialCacheFileName);
            s_logger.Info($"Setting the json credential cache path to {_jsonCacheFilePath}");
        }

        internal void WriteToJsonFile(string content)
        {
            s_logger.Debug($"Writing credentials to json file in {_jsonCacheFilePath}");
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _fileOperations.Write(_jsonCacheFilePath, content);
            }
            else
            {
                if (!_directoryOperations.Exists(_jsonCacheDirectory))
                {
                    _directoryOperations.CreateDirectory(_jsonCacheDirectory);
                }
                s_logger.Info($"Creating the json file for credential cache in {_jsonCacheFilePath}");
                if (_fileOperations.Exists(_jsonCacheFilePath))
                {
                    s_logger.Info($"The existing json file for credential cache in {_jsonCacheFilePath} will be overwritten");
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

        internal KeyTokenDict ReadJsonFile()
        {
            var contentFile = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? File.ReadAllText(_jsonCacheFilePath) : _unixOperations.ReadAllText(_jsonCacheFilePath);
            return JsonConvert.DeserializeObject<KeyTokenDict>(contentFile);
        }

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials from json file in {_jsonCacheFilePath} for key: {key}");
            if (_fileOperations.Exists(_jsonCacheFilePath))
            {
                var keyTokenPairs = ReadJsonFile();
                var hashKey = key.ToSha256Hash();
                if (keyTokenPairs.TryGetValue(hashKey, out string token))
                {
                    return token;
                }
            }

            s_logger.Info("Unable to get credentials for the specified key");
            return "";
        }

        public void RemoveCredentials(string key)
        {
            s_logger.Debug($"Removing credentials from json file in {_jsonCacheFilePath} for key: {key}");
            if (_fileOperations.Exists(_jsonCacheFilePath))
            {
                var keyTokenPairs = ReadJsonFile();
                var hashKey = key.ToSha256Hash();
                keyTokenPairs.Remove(hashKey);
                WriteToJsonFile(JsonConvert.SerializeObject(keyTokenPairs));
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving credentials to json file in {_jsonCacheFilePath} for key: {key}");
            var hashKey = key.ToSha256Hash();
            KeyTokenDict keyTokenPairs = _fileOperations.Exists(_jsonCacheFilePath) ? ReadJsonFile() : new KeyTokenDict();
            keyTokenPairs[hashKey] = token;

            string jsonString = JsonConvert.SerializeObject(keyTokenPairs);
            WriteToJsonFile(jsonString);
        }
    }
}
