/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;

namespace Snowflake.Data.Core.Tools
{
    using System.Runtime.InteropServices;
    using Mono.Unix;

    internal class FileOperations
    {
        public static readonly FileOperations Instance = new FileOperations();
        private readonly UnixOperations _unixOperations = UnixOperations.Instance;

        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }

        public virtual string ReadAllText(string path)
        {
            return ReadAllText(path, FileAccessPermissions.OtherReadWriteExecute);
        }

        public virtual string ReadAllText(string path, FileAccessPermissions? forbiddenPermissions)
        {
            var contentFile = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? File.ReadAllText(path) : _unixOperations.ReadAllText(path, forbiddenPermissions);
            return contentFile;
        }
    }
}
