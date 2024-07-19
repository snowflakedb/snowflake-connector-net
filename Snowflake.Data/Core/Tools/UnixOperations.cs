/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core.Tools
{
    using Mono.Unix;
    using Mono.Unix.Native;
    using System.IO;
    using System.Security;
    using System.Text;

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

        /// <summary>
        /// Reads all text from a file at the specified path, ensuring the file is owned by the effective user and group of the current process,
        /// and does not have broader permissions than specified.
        /// </summary>
        /// <param name="path">The path to the file.</param>
        /// <param name="forbiddenPermissions">Permissions that are not allowed for the file. Defaults to OtherReadWriteExecute.</param>
        /// <returns>The content of the file as a string.</returns>
        /// <exception cref="SecurityException">Thrown if the file is not owned by the effective user or group, or if it has forbidden permissions.</exception>

        public string ReadAllText(string path, FileAccessPermissions? forbiddenPermissions = FileAccessPermissions.OtherReadWriteExecute)
        {
            var fileInfo = new UnixFileInfo(path: path);

            using (var handle = fileInfo.OpenRead())
            {
                if (handle.OwnerUser.UserId != Syscall.geteuid())
                    throw new SecurityException("Attempting to read a file not owned by the effective user of the current process");
                if (handle.OwnerGroup.GroupId != Syscall.getegid())
                    throw new SecurityException("Attempting to read a file not owned by the effective group of the current process");
                if (forbiddenPermissions.HasValue && (handle.FileAccessPermissions & forbiddenPermissions.Value) != 0)
                    throw new SecurityException("Attempting to read a file with too broad permissions assigned");
                using (var streamReader = new StreamReader(handle, Encoding.Default))
                {
                    return streamReader.ReadToEnd();
                }
            }
        }
    }
}
