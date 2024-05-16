/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Mono.Unix.Native;

namespace Snowflake.Data.Core.Tools
{
    internal class UnixOperations
    {
        public static readonly UnixOperations Instance = new UnixOperations();

        public virtual int CreateDirectoryWithPermissions(string path, FilePermissions permissions)
        {
            return Syscall.mkdir(path, permissions);
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
    }
}
