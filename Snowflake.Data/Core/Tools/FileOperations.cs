/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class FileOperations
    {
        public static readonly FileOperations Instance = new FileOperations();

        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }

        public virtual void Write(string path, string content) => File.WriteAllText(path, content);
    }
}
