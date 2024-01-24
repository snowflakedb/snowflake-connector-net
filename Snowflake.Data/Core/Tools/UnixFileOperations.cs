/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;

namespace Snowflake.Data.Core.Tools
{
    internal class UnixFileOperations
    {
        public static readonly UnixFileOperations Instance = new UnixFileOperations();

        private UnixFileInfo _unixFileInfo;

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
