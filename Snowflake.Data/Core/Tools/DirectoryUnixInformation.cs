/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Mono.Unix;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class DirectoryUnixInformation
    {
        private const FileAccessPermissions SafePermissions = FileAccessPermissions.UserReadWriteExecute;
        private const FileAccessPermissions NotSafePermissions = FileAccessPermissions.AllPermissions & ~SafePermissions;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<DirectoryUnixInformation>();

        public string FullName { get; private set; }
        public bool Exists { get; private set; }
        public FileAccessPermissions Permissions { get; private set; }
        public long Owner { get; private set; }

        public DirectoryUnixInformation(UnixDirectoryInfo directoryInfo)
        {
            FullName = directoryInfo.FullName;
            Exists = directoryInfo.Exists;
            if (Exists)
            {
                Permissions = directoryInfo.FileAccessPermissions;
                Owner = directoryInfo.OwnerUserId;
            }
        }

        internal DirectoryUnixInformation(string fullName, bool exists, FileAccessPermissions permissions, long owner)
        {
            FullName = fullName;
            Exists = exists;
            Permissions = permissions;
            Owner = owner;
        }

        public bool IsSafe(long userId)
        {
            if (HasAnyOfPermissions(NotSafePermissions))
            {
                s_logger.Warn($"Directory '{FullName}' permissions are too broad. It could be potentially accessed by group or others.");
                return false;
            }
            if (!IsOwnedBy(userId))
            {
                s_logger.Warn($"Directory '{FullName}' is not owned by the current user.");
                return false;
            }
            return true;
        }

        public bool IsSafeExactly(long userId)
        {
            if (SafePermissions != Permissions)
            {
                s_logger.Warn($"Directory '{FullName}' permissions are different than 700.");
                return false;
            }
            if (!IsOwnedBy(userId))
            {
                s_logger.Warn($"Directory '{FullName}' is not owned by the current user.");
                return false;
            }
            return true;
        }


        private bool HasAnyOfPermissions(FileAccessPermissions permissions) => (permissions & Permissions) != 0;

        private bool IsOwnedBy(long userId) => Owner == userId;


    }
}
