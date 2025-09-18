using System;
using System.IO;
using System.Security;
using System.Text;
using System.Web;
using Mono.Unix;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Revocation
{
    internal class FileCrlCache : ICrlCache
    {
        private static readonly object _lock = new object();
        private const FileAccessPermissions UnwantedUnixPermissions = FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite;
        private const FileAccessPermissions WantedUnixDirPermissions = FileAccessPermissions.UserReadWriteExecute;
        private const FileAccessPermissions WantedUnixFilePermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<FileCrlCache>();

        private readonly FileCrlCacheConfig _config;
        private readonly CrlParser _parser;
        private readonly FileOperations _fileOperations;
        private readonly UnixOperations _unixOperations;
        private readonly DirectoryOperations _directoryOperations;

        internal FileCrlCache(FileCrlCacheConfig config, CrlParser parser, FileOperations fileOperations, UnixOperations unixOperations, DirectoryOperations directoryOperations)
        {
            _config = config;
            _parser = parser;
            _fileOperations = fileOperations;
            _unixOperations = unixOperations;
            _directoryOperations = directoryOperations;
        }

        public static FileCrlCache CreateInstance()
        {
            var config = new FileCrlCacheConfig(EnvironmentOperations.Instance, UnixOperations.Instance);
            var parser = new CrlParser(EnvironmentOperations.Instance);
            return new FileCrlCache(config, parser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance);
        }

        public Crl Get(string crlUrl)
        {
            s_logger.Debug($"Reading crl from file cache for url: {crlUrl}");
            try
            {
                lock (_lock)
                {
                    var filePath = ResolveFilePath(crlUrl);
                    byte[] fileBytes;
                    FileInformation fileInfo;
                    try
                    {
                        fileBytes = ReadCrlBytes(filePath);
                        if (fileBytes == null)
                            return null;
                        fileInfo = _fileOperations.GetFileInfo(filePath);
                    }
                    catch (Exception exception)
                    {
                        s_logger.Error($"Ignored the cache file for crl url: {filePath} because of a reading error: {exception.Message}.", exception);
                        return null;
                    }

                    try
                    {
                        return _parser.Parse(fileBytes, fileInfo.LastWriteTimeUtc);
                    }
                    catch (Exception exception)
                    {
                        s_logger.Error(
                            $"Ignored the cache file for crl url: {filePath} because its content could not be parsed: {exception.Message}.", exception);
                        return null;
                    }
                }
            }
            catch (Exception exception)
            {
                s_logger.Error($"Ignored the cache file for crl url: ${crlUrl} because of unexpected error: {exception.Message}", exception);
                return null;
            }
        }

        public void Set(string crlUrl, Crl crl)
        {
            s_logger.Debug($"Updating file crl cache for crl url: {crlUrl}");
            try
            {
                lock (_lock)
                {
                    var filePath = ResolveFilePath(crlUrl);
                    if (!MakeCacheDirectorySecure())
                    {
                        s_logger.Error($"Skipping saving crl cache for url: {crlUrl} because could not make the cache directory secure.");
                        return;
                    }
                    if (!WriteCrlBytes(filePath, crl.GetEncoded()))
                    {
                        s_logger.Error($"Failed to write crl file for url: {crlUrl}.");
                        return;
                    }
                    SetLastWriteTime(filePath, crl.DownloadTime);
                }
            }
            catch (Exception exception)
            {
                s_logger.Error($"Saving cache file for crl ulr: {crlUrl} interrupted by an unexpected error: {exception.Message}", exception);
            }
        }

        private bool WriteCrlBytes(string filePath, byte[] fileBytes)
        {
            try
            {
                if (_config.IsWindows)
                {
                    _fileOperations.WriteAllBytes(filePath, fileBytes);
                    return true;
                }
                var fileInfo = _unixOperations.GetFileInfo(filePath);
                if (fileInfo.Exists && !fileInfo.IsSafe(_config.UnixUserId, UnwantedUnixPermissions))
                {
                    if (!fileInfo.IsOwnedBy(_config.UnixUserId))
                    {
                        s_logger.Debug($"Changing owner of a file {filePath}");
                        var result = _unixOperations.ChangeOwner(filePath, (int)_config.UnixUserId, (int)_config.UnixGroupId);
                        if (result != 0)
                        {
                            s_logger.Error($"Failed to change ownership of file: {filePath}. Result of chown: {result}");
                            return false;
                        }
                    }

                    if (fileInfo.HasAnyOfPermissions(UnwantedUnixPermissions))
                    {
                        s_logger.Debug($"Changing permission of a file {filePath}");
                        var result = _unixOperations.ChangePermissions(filePath, WantedUnixFilePermissions);
                        if (result != 0)
                        {
                            s_logger.Error($"Failed to change permissions of file: {filePath}. Result of chmod: {result}");
                            return false;
                        }
                    }
                }
                _unixOperations.WriteAllBytes(filePath, fileBytes, ValidateFileNotWritableByOthers);
                return true;
            }
            catch (Exception exception)
            {
                s_logger.Error($"Failed to write file crl url: {filePath} because: {exception.Message}.", exception);
                return false;
            }
        }

        private void SetLastWriteTime(string filePath, DateTime lastWriteTimeUtc)
        {
            try
            {
                _fileOperations.SetLastWriteTimeUtc(filePath, lastWriteTimeUtc);
            }
            catch (Exception exception)
            {
                s_logger.Error($"Failed to set file last write time: {exception.Message}.", exception);
            }
        }

        private string ResolveFilePath(string crlUrl)
        {
            var encodedUrl = HttpUtility.UrlEncode(crlUrl, Encoding.UTF8);
            return Path.Combine(_config.DirectoryPath, encodedUrl);
        }

        private byte[] ReadCrlBytes(string filePath)
        {
            if (!_fileOperations.Exists(filePath))
                return null;
            if (_config.IsWindows)
            {
                return _fileOperations.ReadAllBytes(filePath);
            }
            return _unixOperations.ReadAllBytes(filePath, ValidateFileNotWritableByOthers);
        }

        private bool MakeCacheDirectorySecure()
        {
            try
            {
                if (_config.IsWindows)
                {
                    if (!_directoryOperations.Exists(_config.DirectoryPath))
                    {
                        _directoryOperations.CreateDirectory(_config.DirectoryPath);
                    }
                    return true;
                }
                var directoryInfo = _unixOperations.GetDirectoryInfo(_config.DirectoryPath);
                if (directoryInfo.Exists && !directoryInfo.IsSafe(_config.UnixUserId, UnwantedUnixPermissions))
                {
                    s_logger.Warn($"Directory {_config.DirectoryPath} has unsafe permissions. Changing it.");
                    if (!directoryInfo.IsOwnedBy(_config.UnixUserId))
                    {
                        s_logger.Debug($"Changing owner of a directory {_config.DirectoryPath}");
                        var result = _unixOperations.ChangeOwner(_config.DirectoryPath, (int)_config.UnixUserId, (int)_config.UnixGroupId);
                        if (result != 0)
                        {
                            s_logger.Error($"Failed to change ownership of directory: {_config.DirectoryPath}. Result of chown: {result}");
                            return false;
                        }
                    }
                    if (directoryInfo.HasAnyOfPermissions(UnwantedUnixPermissions))
                    {
                        s_logger.Debug($"Changing permission of a directory {_config.DirectoryPath}");
                        var result = _unixOperations.ChangePermissions(_config.DirectoryPath, WantedUnixDirPermissions);
                        if (result != 0)
                        {
                            s_logger.Error($"Failed to change permissions of directory: {_config.DirectoryPath}. Result of chmod: {result}");
                            return false;
                        }
                    }
                }
                else if (!directoryInfo.Exists)
                {
                    _directoryOperations.CreateDirectory(_config.DirectoryPath);
                }
                return true;
            }
            catch (Exception exception)
            {
                s_logger.Error($"Failed to make secure crl cache directory: {_config.DirectoryPath}: {exception.Message}", exception);
                return false;
            }
        }

        internal void ValidateFileNotWritableByOthers(UnixStream stream)
        {
            if (stream.OwnerUserId != _config.UnixUserId)
            {
                var errorMessage = $"Error due to user not having ownership of the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }

            if (stream.OwnerGroupId != _config.UnixGroupId)
            {
                var errorMessage = $"Error due to group not having ownership of the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }

            if (_unixOperations.CheckFileHasAnyOfPermissions(stream.FileAccessPermissions, UnwantedUnixPermissions))
            {
                var errorMessage = $"Error due to other users having permission to modify the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }
        }
    }
}
