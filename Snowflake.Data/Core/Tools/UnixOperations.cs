/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Mono.Unix.Native;
using System.IO;
using System.Text;

namespace Snowflake.Data.Core.Tools
{
    internal class UnixOperations
    {
        public static readonly UnixOperations Instance = new UnixOperations();

        public virtual int CreateFileWithPermissions(string path, FilePermissions permissions)
        {
            return Syscall.creat(path, permissions);
        }

        public virtual string ReadFile(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            using (var handle = fileInfo.OpenRead())
            {
                using (var streamReader = new StreamReader(handle, Encoding.Default))
                {
                    return streamReader.ReadToEnd();
                }
            }
        }

        public virtual int CreateDirectoryWithPermissions(string path, FilePermissions permissions)
        {
            return Syscall.mkdir(path, permissions);
        }

        public virtual FileAccessPermissions GetFilePermissions(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            return fileInfo.FileAccessPermissions;
        }

        public virtual FileAccessPermissions GetDirPermissions(string path)
        {
            var dirInfo = new UnixDirectoryInfo(path);
            return dirInfo.FileAccessPermissions;
        }

        public virtual bool CheckFileHasAnyOfPermissions(string path, FileAccessPermissions permissions)
        {
            var fileInfo = new UnixFileInfo(path);
            return (permissions & fileInfo.FileAccessPermissions) != 0;
        }

        public virtual bool CheckFileIsNotOwnedByCurrentUser(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            using (var handle = fileInfo.OpenRead())
            {
                return handle.OwnerUser.UserId != Syscall.geteuid();
            }
        }

        public virtual bool CheckFileIsNotOwnedByCurrentGroup(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            using (var handle = fileInfo.OpenRead())
            {
                return handle.OwnerGroup.GroupId != Syscall.getegid();
            }
        }
    }
}
