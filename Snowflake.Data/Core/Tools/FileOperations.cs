/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class FileOperations
    {
        public static readonly FileOperations Instance = new FileOperations();

        private UnixFileInfo _unixFileInfo;

        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }

        public virtual void SetUnixFileInfo(string path)
        {
            _unixFileInfo = new UnixFileInfo(path);
        }

        public virtual bool HasFlag(FileAccessPermissions permissions)
        {
            return _unixFileInfo.FileAccessPermissions.HasFlag(permissions);
        }
    }
}
