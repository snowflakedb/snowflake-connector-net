/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Mono.Unix.Native;
using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class UnixOperations
    {
        public static readonly UnixOperations Instance = new UnixOperations();

        private UnixFileInfo _unixFileInfo;
        private UnixDirectoryInfo _unixDirInfo;

        public virtual void SetDirInfo(string path)
        {
            _unixDirInfo = new UnixDirectoryInfo(path);
        }

        public virtual void CreateDirectoryWithPermissions(string path, FilePermissions permissions)
        {
            string subPath = Path.GetDirectoryName(path);
            if (!Directory.Exists(subPath))
            {
                Directory.CreateDirectory(subPath);
            }
            Syscall.mkdir(path, permissions);
        }

        public virtual FileAccessPermissions GetDirPermissions()
        {
            return _unixDirInfo.FileAccessPermissions;
        }

        public virtual void SetFileInfo(string path)
        {
            _unixFileInfo = new UnixFileInfo(path);
        }

        public virtual bool CheckFileHasPermissions(FileAccessPermissions permissions)
        {
            return _unixFileInfo.FileAccessPermissions.HasFlag(permissions);
        }
    }
}
