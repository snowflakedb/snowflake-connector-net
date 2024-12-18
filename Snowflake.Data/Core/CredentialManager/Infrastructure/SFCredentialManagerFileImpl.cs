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
using KeyTokenDict = System.Collections.Generic.Dictionary<string, string>;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerFileImpl : ISnowflakeCredentialManager
    {
        internal const int CredentialCacheLockDurationSeconds = 60;

        internal const FilePermissions CredentialCacheLockDirPermissions = FilePermissions.S_IRUSR;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFileImpl>();

        private static readonly object s_lock = new object();

        internal SFCredentialManagerFileStorage _fileStorage = null;

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
        }

        internal void WriteToJsonFile(string content)
        {
            s_logger.Debug($"Writing credentials to json file in {_fileStorage.JsonCacheFilePath}");
            if (!_directoryOperations.Exists(_fileStorage.JsonCacheDirectory))
            {
                _directoryOperations.CreateDirectory(_fileStorage.JsonCacheDirectory);
            }
            if (_fileOperations.Exists(_fileStorage.JsonCacheFilePath))
            {
                s_logger.Info($"The existing json file for credential cache in {_fileStorage.JsonCacheFilePath} will be overwritten");
            }
            else
            {
                s_logger.Info($"Creating the json file for credential cache in {_fileStorage.JsonCacheFilePath}");
                var createFileResult = _unixOperations.CreateFileWithPermissions(_fileStorage.JsonCacheFilePath,
                    FilePermissions.S_IRUSR | FilePermissions.S_IWUSR);
                if (createFileResult == -1)
                {
                    var errorMessage = "Failed to create the JSON token cache file";
                    s_logger.Error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }
            _fileOperations.Write(_fileStorage.JsonCacheFilePath, content, ValidateFilePermissions);
        }

        internal KeyTokenDict ReadJsonFile()
        {
            var contentFile = _fileOperations.ReadAllText(_fileStorage.JsonCacheFilePath, ValidateFilePermissions);
            try
            {
                var fileContent = JsonConvert.DeserializeObject<CredentialsFileContent>(contentFile);
                return (fileContent == null || fileContent.Tokens == null) ? new KeyTokenDict() : fileContent.Tokens;
            }
            catch (Exception)
            {
                s_logger.Error("Failed to parse the file with cached credentials");
                return new KeyTokenDict();
            }
        }

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials for key: {key}");
            lock (s_lock)
            {
                InitializeFileStorageIfNeeded();
                s_logger.Debug($"Getting credentials from json file in {_fileStorage.JsonCacheFilePath} for key: {key}");
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for reading credentials");
                    return string.Empty;
                }
                try
                {
                    if (_fileOperations.Exists(_fileStorage.JsonCacheFilePath))
                    {
                        var keyTokenPairs = ReadJsonFile();
                        if (keyTokenPairs.TryGetValue(key, out string token))
                        {
                            return token;
                        }
                    }
                }
                catch (Exception exception)
                {
                    s_logger.Error("Failed to get credentials", exception);
                    throw;
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
            s_logger.Debug($"Removing credentials for key: {key}");
            lock (s_lock)
            {
                InitializeFileStorageIfNeeded();
                s_logger.Debug($"Removing credentials from json file in {_fileStorage.JsonCacheFilePath} for key: {key}");
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for removing credentials");
                    return;
                }
                try
                {
                    if (_fileOperations.Exists(_fileStorage.JsonCacheFilePath))
                    {
                        var keyTokenPairs = ReadJsonFile();
                        keyTokenPairs.Remove(key);
                        var credentials = new CredentialsFileContent { Tokens = keyTokenPairs };
                        WriteToJsonFile(JsonConvert.SerializeObject(credentials));
                    }
                }
                catch (Exception exception)
                {
                    s_logger.Error("Failed to remove credentials", exception);
                    throw;
                }
                finally
                {
                    ReleaseLock();
                }
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving credentials for key: {key}");
            lock (s_lock)
            {
                InitializeFileStorageIfNeeded();
                s_logger.Debug($"Saving credentials to json file in {_fileStorage.JsonCacheFilePath} for key: {key}");
                var lockAcquired = AcquireLockWithRetries(); // additional fs level locking is to synchronize file access across many applications
                if (!lockAcquired)
                {
                    s_logger.Error("Failed to acquire lock for saving credentials");
                    return;
                }
                try
                {
                    KeyTokenDict keyTokenPairs = _fileOperations.Exists(_fileStorage.JsonCacheFilePath) ? ReadJsonFile() : new KeyTokenDict();
                    keyTokenPairs[key] = token;
                    var credentials = new CredentialsFileContent { Tokens = keyTokenPairs };
                    string jsonString = JsonConvert.SerializeObject(credentials);
                    WriteToJsonFile(jsonString);
                }
                catch (Exception exception)
                {
                    s_logger.Error("Failed to save credentials", exception);
                    throw;
                }
                finally
                {
                    ReleaseLock();
                }
            }
        }

        private void InitializeFileStorageIfNeeded()
        {
            if (_fileStorage != null)
                return;
            var fileStorage = new SFCredentialManagerFileStorage(_environmentOperations);
            PrepareParentDirectory(fileStorage.JsonCacheDirectory);
            PrepareSecureDirectory(fileStorage.JsonCacheDirectory);
            _fileStorage = fileStorage;
        }

        private void PrepareParentDirectory(string directory)
        {
            var parentDirectory = _directoryOperations.GetParentDirectoryInfo(directory);
            if (!parentDirectory.Exists)
            {
                _directoryOperations.CreateDirectory(parentDirectory.FullName);
            }
        }

        private void PrepareSecureDirectory(string directory)
        {
            var unixDirectoryInfo = _unixOperations.GetDirectoryInfo(directory);
            if (unixDirectoryInfo.Exists)
            {
                var userId = _unixOperations.GetCurrentUserId();
                if (!unixDirectoryInfo.IsSafeExactly(userId))
                {
                    SetSecureOwnershipAndPermissions(directory, userId);
                }
            }
            else
            {
                var createResult = _unixOperations.CreateDirectoryWithPermissions(directory, FilePermissions.S_IRWXU);
                if (createResult == -1)
                {
                    throw new SecurityException($"Could not create directory: {directory}");
                }
            }
        }

        private void SetSecureOwnershipAndPermissions(string directory, long userId)
        {
            var groupId = _unixOperations.GetCurrentGroupId();
            var chownResult = _unixOperations.ChangeOwner(directory, (int) userId, (int) groupId);
            if (chownResult == -1)
            {
                throw new SecurityException($"Could not set proper directory ownership for directory: {directory}");
            }
            var chmodResult = _unixOperations.ChangePermissions(directory, FileAccessPermissions.UserReadWriteExecute);
            if (chmodResult == -1)
            {
                throw new SecurityException($"Could not set proper directory permissions for directory: {directory}");
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
            if (!_directoryOperations.Exists(_fileStorage.JsonCacheDirectory))
            {
                _directoryOperations.CreateDirectory(_fileStorage.JsonCacheDirectory);
            }
            var lockDirectoryInfo = _directoryOperations.GetDirectoryInfo(_fileStorage.JsonCacheLockPath);
            if (lockDirectoryInfo.IsCreatedEarlierThanSeconds(CredentialCacheLockDurationSeconds, DateTime.UtcNow))
            {
                s_logger.Warn($"File cache lock directory {_fileStorage.JsonCacheLockPath} created more than {CredentialCacheLockDurationSeconds} seconds ago. Removing the lock directory.");
                ReleaseLock();
            }
            else if (lockDirectoryInfo.Exists)
            {
                return false;
            }
            var result = _unixOperations.CreateDirectoryWithPermissions(_fileStorage.JsonCacheLockPath, CredentialCacheLockDirPermissions);
            return result == 0;
        }

        private void ReleaseLock()
        {
            _directoryOperations.Delete(_fileStorage.JsonCacheLockPath, false);
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
