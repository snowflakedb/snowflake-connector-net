/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Core.Tools
{
    internal class DirectoryOperations
    {
        public static readonly DirectoryOperations Instance = new DirectoryOperations();
        private readonly UnixOperations _unixOperations;

        internal DirectoryOperations() : this(UnixOperations.Instance)
        {
        }

        internal DirectoryOperations(UnixOperations unixOperations)
        {
            _unixOperations = unixOperations;
        }

        public virtual bool Exists(string path) => Directory.Exists(path);

        public virtual DirectoryInfo CreateDirectory(string path) => Directory.CreateDirectory(path);

        public virtual void Delete(string path, bool recursive) => Directory.Delete(path, recursive);

        public virtual DirectoryInformation GetDirectoryInfo(string path) => new DirectoryInformation(new DirectoryInfo(path));

        public virtual DirectoryInformation GetParentDirectoryInfo(string path) => new DirectoryInformation(Directory.GetParent(path));

        public virtual bool IsDirectorySafe(string path)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return true;
            }
            var unixInfo = _unixOperations.GetDirectoryInfo(path);
            return unixInfo.IsSafe(_unixOperations.GetCurrentUserId());
        }
    }
}
