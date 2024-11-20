/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using Mono.Unix;
using Mono.Unix.Native;

namespace Snowflake.Data.Core.Tools
{

    internal class UnixOperations
    {
        public static readonly UnixOperations Instance = new UnixOperations();

        public virtual int CreateFileWithPermissions(string path, FilePermissions permissions)
        {
            return Syscall.creat(path, permissions);
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

        public string ReadAllText(string path, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);

            using (var handle = fileInfo.OpenRead())
            {
                validator?.Invoke(handle);
                using (var streamReader = new StreamReader(handle, Encoding.UTF8))
                {
                    return streamReader.ReadToEnd();
                }
            }
        }

        public void WriteAllText(string path, string content, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);

            using (var handle = fileInfo.OpenRead())
            {
                validator?.Invoke(handle);
            }
            File.WriteAllText(path, content);
        }

        internal static void ValidateFileWhenReadIsAccessedOnlyByItsOwner(UnixStream stream)
        {
            var allowedPermissions = new[]
            {
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite,
                FileAccessPermissions.UserRead
            };
            if (stream.OwnerUser.UserId != Syscall.geteuid())
                throw new SecurityException("Attempting to read a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != Syscall.getegid())
                throw new SecurityException("Attempting to read a file not owned by the effective group of the current process");
            if (!(allowedPermissions.Any(a => stream.FileAccessPermissions == a)))
                throw new SecurityException("Attempting to read a file with too broad permissions assigned");
        }

        internal static void ValidateFileWhenWriteIsAccessedOnlyByItsOwner(UnixStream stream)
        {
            var allowedPermissions = new[]
            {
                FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite
            };
            if (stream.OwnerUser.UserId != Syscall.geteuid())
                throw new SecurityException("Attempting to write a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != Syscall.getegid())
                throw new SecurityException("Attempting to write a file not owned by the effective group of the current process");
            if (!(allowedPermissions.Any(a => stream.FileAccessPermissions == a)))
                throw new SecurityException("Attempting to write a file with too broad permissions assigned");
        }
    }
}
