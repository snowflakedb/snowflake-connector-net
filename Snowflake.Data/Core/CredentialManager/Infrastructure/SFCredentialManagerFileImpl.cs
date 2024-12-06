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
using System.Linq;
using System.Security;
using System.Threading;
using Newtonsoft.Json.Linq;
using KeyTokenDict = System.Collections.Generic.Dictionary<string, string>;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerFileImpl : ISnowflakeCredentialManager
    {
        internal const string CredentialCacheDirectoryEnvironmentName = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";

        internal const string CredentialCacheDirName = ".snowflake";

        internal const string CredentialCacheFileName = "credential_cache.json";

        internal const string CredentialCacheLockName = "credential_cache.json.lck";

        internal const FilePermissions CredentialCacheLockDirPermissions = FilePermissions.S_IRUSR;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFileImpl>();

        private static readonly object s_lock = new object();

        private readonly string _jsonCacheDirectory;

        private readonly string _jsonCacheFilePath;

        private readonly string _jsonCacheLockPath;

        private readonly FileOperations _fileOperations;

        private readonly DirectoryOperations _directoryOperations;

        private readonly UnixOperations _unixOperations;

        private readonly EnvironmentOperations _environmentOperations;

        public static readonly SFCredentialManagerFileImpl Instance = new SFCredentialManagerFileImpl(FileOperations.Instance, DirectoryOperations.Instance, UnixOperations.Instance, EnvironmentOperations.Instance);

        internal SFCredentialManagerFileImpl(FileOperations fileOperations, DirectoryOperations directoryOperations, UnixOperations unixOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _directoryOperations = directoryOperations;
            _unixOperations = unixOperations;
            _environmentOperations = environmentOperations;
            SetCredentialCachePath(ref _jsonCacheDirectory, ref _jsonCacheFilePath, ref _jsonCacheLockPath);
        }

        private void SetCredentialCachePath(ref string _jsonCacheDirectory, ref string _jsonCacheFilePath, ref string _jsonCacheLockPath)
        {
            var customDirectory = _environmentOperations.GetEnvironmentVariable(CredentialCacheDirectoryEnvironmentName);
            _jsonCacheDirectory = string.IsNullOrEmpty(customDirectory) ? Path.Combine(HomeDirectoryProvider.HomeDirectory(_environmentOperations), CredentialCacheDirName) : customDirectory;
            if (!_directoryOperations.Exists(_jsonCacheDirectory))
            {
                _directoryOperations.CreateDirectory(_jsonCacheDirectory);
            }
            _jsonCacheFilePath = Path.Combine(_jsonCacheDirectory, CredentialCacheFileName);
            _jsonCacheLockPath = Path.Combine(_jsonCacheDirectory, CredentialCacheLockName);
            s_logger.Info($"Setting the json credential cache path to {_jsonCacheFilePath}");
        }

        internal void WriteToJsonFile(string content)
        {
            s_logger.Debug($"Writing credentials to json file in {_jsonCacheFilePath}");
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
                FilePermissions.S_IRUSR | FilePermissions.S_IWUSR);
            if (createFileResult == -1)
            {
                var errorMessage = "Failed to create the JSON token cache file";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            _fileOperations.Write(_jsonCacheFilePath, content, ValidateFilePermissions);
        }

        internal KeyTokenDict ReadJsonFile()
        {
            var contentFile = _fileOperations.ReadAllText(_jsonCacheFilePath, ValidateFilePermissions);
            try
            {
                JObject.Parse(contentFile);
                var fileContent = JsonConvert.DeserializeObject<CredentialsFileContent>(contentFile);
                return fileContent == null ? new KeyTokenDict() : fileContent.Tokens;
            }
            catch (Exception)
            {
                s_logger.Error("Failed to parse the file with cached credentials");
                return new KeyTokenDict();
            }
        }

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials from json file in {_jsonCacheFilePath} for key: {key}");
            lock (s_lock)
            {
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for reading credentials");
                    return string.Empty;
                }
                try
                {
                    if (_fileOperations.Exists(_jsonCacheFilePath))
                    {
                        var keyTokenPairs = ReadJsonFile();
                        if (keyTokenPairs.TryGetValue(key, out string token))
                        {
                            return token;
                        }
                    }
                }
                finally
                {
                    ReleaseLock();
                }
            }
            s_logger.Info("Unable to get credentials for the specified key");
            return string.Empty;
        }

        public void RemoveCredentials(string key)
        {
            s_logger.Debug($"Removing credentials from json file in {_jsonCacheFilePath} for key: {key}");
            lock (s_lock)
            {
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for removing credentials");
                    return;
                }
                try
                {
                    if (_fileOperations.Exists(_jsonCacheFilePath))
                    {
                        var keyTokenPairs = ReadJsonFile();
                        keyTokenPairs.Remove(key);
                        WriteToJsonFile(JsonConvert.SerializeObject(keyTokenPairs));
                    }
                }
                finally
                {
                    ReleaseLock();
                }
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving credentials to json file in {_jsonCacheFilePath} for key: {key}");
            lock (s_lock)
            {
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for saving credentials");
                    return;
                }
                try
                {
                    KeyTokenDict keyTokenPairs = _fileOperations.Exists(_jsonCacheFilePath) ? ReadJsonFile() : new KeyTokenDict();
                    keyTokenPairs[key] = token;

                    string jsonString = JsonConvert.SerializeObject(keyTokenPairs);
                    WriteToJsonFile(jsonString);
                }
                finally
                {
                    ReleaseLock();
                }
            }
        }

        private bool AcquireLockWithRetries() => AcquireLock(5, TimeSpan.FromMilliseconds(50));

        private bool AcquireLock(int numberOfAttempts, TimeSpan delayTime)
        {
            for (var i = 0; i < numberOfAttempts; i++)
            {
                if (AcquireLock())
                    return true;
                if (i + 1 < numberOfAttempts)
                    Thread.Sleep(delayTime);
            }
            return false;
        }

        private bool AcquireLock()
        {
            if (!_directoryOperations.Exists(_jsonCacheDirectory))
            {
                _directoryOperations.CreateDirectory(_jsonCacheDirectory);
            }
            if (_directoryOperations.Exists(_jsonCacheLockPath))
                return false;
            var result = _unixOperations.CreateDirectoryWithPermissions(_jsonCacheLockPath, CredentialCacheLockDirPermissions);
            return result == 0;
        }

        private void ReleaseLock()
        {
            _directoryOperations.Delete(_jsonCacheLockPath, false);
        }

        internal static void ValidateFilePermissions(UnixStream stream)
        {
            var allowedPermissions = new[]
            {
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite
            };
            if (stream.OwnerUser.UserId != Syscall.geteuid())
                throw new SecurityException("Attempting to read or write a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != Syscall.getegid())
                throw new SecurityException("Attempting to read or write a file not owned by the effective group of the current process");
            if (!(allowedPermissions.Any(a => stream.FileAccessPermissions == a)))
                throw new SecurityException("Attempting to read or write a file with too broad permissions assigned");
        }
    }
}
