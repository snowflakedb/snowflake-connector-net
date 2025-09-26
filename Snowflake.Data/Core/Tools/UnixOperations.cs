using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public virtual FileUnixInformation GetFileInfo(string path)
        {
            var fileInfo = new UnixFileInfo(path);
            return new FileUnixInformation(fileInfo);
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

        public virtual byte[] ReadAllBytes(string path, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);
            const int BufferSize = 200 * 1024; // 200kB
            var result = new List<byte[]>();
            using (var handle = fileInfo.OpenRead())
            {
                validator?.Invoke(handle);
                using (var streamReader = new BinaryReader(handle, Encoding.ASCII))
                {
                    bool allRead = false;
                    while (!allRead)
                    {
                        var bytes = streamReader.ReadBytes(BufferSize);
                        if (bytes.Length > 0)
                        {
                            result.Add(bytes);
                        }
                        allRead = bytes.Length < BufferSize;
                    }
                }
            }
            return JoinArrays(result);
        }

        private byte[] JoinArrays(List<byte[]> arrays)
        {
            if (arrays == null || arrays.Count == 0)
                return Array.Empty<byte>();
            if (arrays.Count == 1)
                return arrays[0];
            var totalLength = arrays.Select(x => x.Length).Sum();
            var result = new byte[totalLength];
            var index = 0;
            foreach (var array in arrays)
            {
                Array.Copy(array, 0, result, index, array.Length);
                index += array.Length;
            }
            return result;
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

        public virtual void WriteAllBytes(string path, byte[] bytes, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);
            using (var handle = fileInfo.Open(FileMode.Create, FileAccess.ReadWrite, FilePermissions.S_IWUSR | FilePermissions.S_IRUSR))
            {
                validator?.Invoke(handle);
                using (var writer = new BinaryWriter(handle, Encoding.ASCII))
                {
                    writer.Write(bytes);
                }
            }
        }

        public long AppendToFile(string path, string mainContent, string additionalContent, Action<UnixStream> validator)
        {
            var fileInfo = new UnixFileInfo(path: path);
            using (var handle = fileInfo.Open(FileMode.Append, FileAccess.ReadWrite, FilePermissions.S_IWUSR | FilePermissions.S_IRUSR))
            {
                validator?.Invoke(handle);
                using (var streamWriter = new StreamWriter(handle, Encoding.UTF8))
                {
                    streamWriter.Write(mainContent);
                    if (additionalContent != null)
                        streamWriter.Write(additionalContent);
                }
            }
            return fileInfo.Length;
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
