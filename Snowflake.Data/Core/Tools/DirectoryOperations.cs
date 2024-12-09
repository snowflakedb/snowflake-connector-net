/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class DirectoryOperations
    {
        public static readonly DirectoryOperations Instance = new DirectoryOperations();

        public virtual bool Exists(string path) => Directory.Exists(path);

        public virtual DirectoryInfo CreateDirectory(string path) => Directory.CreateDirectory(path);

        public virtual void Delete(string path, bool recursive) => Directory.Delete(path, recursive);

        public virtual DirectoryInformation GetDirectoryInfo(string path) => new DirectoryInformation(new DirectoryInfo(path));
    }
}
