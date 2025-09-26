using Mono.Unix;

namespace Snowflake.Data.Core.Tools
{
    internal abstract class FileOrDirectoryUnixInformation
    {
        public string FullName { get; protected set; }
        public bool Exists { get; protected set; }
        public FileAccessPermissions Permissions { get; protected set; }
        public long Owner { get; protected set; }

        internal FileOrDirectoryUnixInformation()
        {
        }

        internal FileOrDirectoryUnixInformation(string fullName, bool exists, FileAccessPermissions permissions, long owner)
        {
            FullName = fullName;
            Exists = exists;
            Permissions = permissions;
            Owner = owner;
        }

        public bool IsSafe(long userId, FileAccessPermissions forbiddenPermissions)
        {
            return IsOwnedBy(userId) && !HasAnyOfPermissions(forbiddenPermissions);
        }

        public bool HasAnyOfPermissions(FileAccessPermissions permissions) => (permissions & Permissions) != 0;

        public bool IsOwnedBy(long userId) => Owner == userId;
    }
}
