using Mono.Unix;

namespace Snowflake.Data.Core.Tools
{
    internal class FileUnixInformation : FileOrDirectoryUnixInformation
    {
        public FileUnixInformation(UnixFileInfo unixFileInfo) : base()
        {
            FullName = unixFileInfo.FullName;
            Exists = unixFileInfo.Exists;
            if (Exists)
            {
                Permissions = unixFileInfo.FileAccessPermissions;
                Owner = unixFileInfo.OwnerUserId;
            }
        }

        public FileUnixInformation(string fullName, bool exists, FileAccessPermissions permissions, long owner) : base(fullName, exists, permissions, owner)
        {
        }
    }
}
