using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;

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

        public virtual string CreateDirectory(string path)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Directory.CreateDirectory(path);
            }
            else
            {
                _unixOperations.CreateDirectoryWithPermissions(path, FileAccessPermissions.UserReadWriteExecute);
            }

            return path;
        }

        public virtual void Delete(string path, bool recursive) => Directory.Delete(path, recursive);

        public virtual DirectoryInformation GetDirectoryInfo(string path) => new DirectoryInformation(new DirectoryInfo(path));

        public virtual DirectoryInformation GetParentDirectoryInfo(string path) => new DirectoryInformation(Directory.GetParent(path));

        public virtual string[] GetFiles(string path, string searchPattern) => Directory.GetFiles(path, searchPattern);

        public virtual bool IsDirectorySafe(string path)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return true;
            }
            var unixInfo = _unixOperations.GetDirectoryInfo(path);
            return unixInfo.IsSafe(_unixOperations.GetCurrentUserId());
        }

        public virtual bool IsDirectoryOwnedByCurrentUser(string path)
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ||
                   _unixOperations.GetOwnerIdOfDirectory(path) == _unixOperations.GetCurrentUserId();
        }
    }
}
