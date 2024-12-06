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

            using (var handle = fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FilePermissions.S_IWUSR |  FilePermissions.S_IRUSR))
            {
                validator?.Invoke(handle);
                using (var streamWriter = new StreamWriter(handle, Encoding.UTF8))
                {
                    streamWriter.Write(content);
                }
            }
        }
    }
}
