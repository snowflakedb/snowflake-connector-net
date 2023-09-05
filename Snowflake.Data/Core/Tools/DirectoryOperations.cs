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
    }
}
