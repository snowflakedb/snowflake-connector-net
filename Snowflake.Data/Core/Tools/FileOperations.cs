/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;

namespace Snowflake.Data.Core.Tools
{

    internal class FileOperations
    {
        public static readonly FileOperations Instance = new FileOperations();
        private readonly UnixOperations _unixOperations = UnixOperations.Instance;

        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }

        public virtual void Write(string path, string content, Action<UnixStream> validator = null)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || validator == null)
            {
                File.WriteAllText(path, content);
            }
            else
            {
                _unixOperations.WriteAllText(path, content, validator);
            }
        }

        public virtual string ReadAllText(string path)
        {
            return ReadAllText(path, null);
        }

        public virtual string ReadAllText(string path, Action<UnixStream> validator)
        {
            var contentFile = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || validator == null ? File.ReadAllText(path) : _unixOperations.ReadAllText(path, validator);
            return contentFile;
        }
    }
}
