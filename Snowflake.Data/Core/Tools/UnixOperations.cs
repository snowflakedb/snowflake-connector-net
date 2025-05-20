using System;
using System.IO;
using System.Text;
using Mono.Unix;
using Mono.Unix.Native;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{

    internal class UnixOperations
    {
        public static readonly UnixOperations Instance = new UnixOperations();
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<UnixOperations>();

        public virtual void CreateDirectoryWithPermissions(string path, FileAccessPermissions permissions)
        {
            var fullPath = Path.GetFullPath(path);
            var splitDirectories = fullPath.Split(Path.DirectorySeparatorChar);

            var dirPath = Path.DirectorySeparatorChar.ToString();
            foreach (var dir in splitDirectories)
            {
                dirPath = Path.Combine(dirPath, dir);

                if (Directory.Exists(dirPath) || dirPath == Path.PathSeparator.ToString())
                {
                    continue;
                }

                CreateSingleDirectory(dirPath, permissions);
            }
        }

        public virtual int CreateDirectoryWithPermissionsMkdir(string path, FileAccessPermissions permissions)
        {
            return Syscall.mkdir(path, (FilePermissions)permissions);
        }

        private static void CreateSingleDirectory(string path, FileAccessPermissions permissions)
        {
            s_logger.Debug($"Creating a directory {path} with permissions: {permissions}");
            try
            {
                new UnixDirectoryInfo(path).Create(permissions);
            }
            catch (Exception e)
            {
                throw new IOException("Unable to create directory", e);
            }
        }

        public virtual Stream CreateFileWithPermissions(string path, FileAccessPermissions permissions)
        {
            var dirPath = Path.GetDirectoryName(path);
            if (dirPath != null)
            {
                CreateDirectoryWithPermissions(dirPath, FileAccessPermissions.UserReadWriteExecute);
            }

            s_logger.Debug($"Creating a file {path} with permissions: {permissions}");
            return new UnixFileInfo(path).Create(permissions);
        }

        public virtual FileAccessPermissions GetFilePermissions(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            return fileInfo.FileAccessPermissions;
        }

        public virtual FileAccessPermissions GetDirPermissions(string path)
        {
            var dirInfo = new UnixDirectoryInfo(path);
            return dirInfo.FileAccessPermissions;
        }

        public virtual DirectoryUnixInformation GetDirectoryInfo(string path)
        {
            var dirInfo = new UnixDirectoryInfo(path);
            return new DirectoryUnixInformation(dirInfo);
        }

        public virtual long ChangeOwner(string path, int userId, int groupId) => Syscall.chown(path, userId, groupId);

        public virtual long ChangePermissions(string path, FileAccessPermissions permissions) => Syscall.chmod(path, (FilePermissions)permissions);

        public virtual bool CheckFileHasAnyOfPermissions(FileAccessPermissions actualPermissions, FileAccessPermissions expectedPermissions)
        {
            return (expectedPermissions & actualPermissions) != 0;
        }

        public virtual bool CheckDirectoryHasAnyOfPermissions(string path, FileAccessPermissions permissions)
        {
            var directoryInfo = new UnixDirectoryInfo(path);
            return (permissions & directoryInfo.FileAccessPermissions) != 0;
        }

        public string ReadAllText(string path, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);

            using (var handle = fileInfo.OpenRead())
            {
                validator?.Invoke(handle);
                using (var streamReader = new StreamReader(handle, Encoding.UTF8))
                {
                    return streamReader.ReadToEnd();
                }
            }
        }

        public void WriteAllText(string path, string content, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);

            using (var handle = fileInfo.Open(FileMode.Create, FileAccess.ReadWrite, FilePermissions.S_IWUSR | FilePermissions.S_IRUSR))
            {
                validator?.Invoke(handle);
                using (var streamWriter = new StreamWriter(handle, Encoding.UTF8))
                {
                    streamWriter.Write(content);
                }
            }
        }

        public virtual long GetCurrentUserId()
        {
            return Syscall.getuid();
        }

        public virtual long GetCurrentGroupId()
        {
            return Syscall.getgid();
        }

        public virtual long GetOwnerIdOfDirectory(string path)
        {
            var dirInfo = new UnixDirectoryInfo(path);
            return dirInfo.OwnerUser.UserId;
        }

        public virtual long GetOwnerIdOfFile(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            return fileInfo.OwnerUser.UserId;
        }
    }
}
