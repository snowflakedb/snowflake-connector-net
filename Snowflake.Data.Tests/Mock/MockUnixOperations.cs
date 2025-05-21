using Mono.Unix;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.Mock
{
    internal class MockUnixOperations : UnixOperations
    {
        public long CurrentUserId { get; set; } = 0;
        public long FileOwnerId { get; set; } = 0;
        public long DirectoryOwnerId { get; set; } = 0;
        public bool DirectoryPermissionsReturn { get; set; } = false;
        public bool FilePermissionsReturn { get; set; } = false;

        public override bool CheckDirectoryHasAnyOfPermissions(string path, FileAccessPermissions permissions)
        {
            return DirectoryPermissionsReturn;
        }

        public override bool CheckFileHasAnyOfPermissions(FileAccessPermissions actualPermissions, FileAccessPermissions expectedPermissions)
        {
            return FilePermissionsReturn;
        }

        public override long GetCurrentUserId()
        {
            return CurrentUserId;
        }

        public override long GetOwnerIdOfDirectory(string path)
        {
            return DirectoryOwnerId;
        }

        public override long GetOwnerIdOfFile(string path)
        {
            return FileOwnerId;
        }
    }
}
