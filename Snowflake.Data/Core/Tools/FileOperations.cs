using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using Mono.Unix;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class FileOperations
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<FileOperations>();
        public static readonly FileOperations Instance = new FileOperations();
        private readonly UnixOperations _unixOperations;
        private const FileAccessPermissions NotSafePermissions = FileAccessPermissions.AllPermissions & ~(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite);

        internal FileOperations() : this(UnixOperations.Instance)
        {
        }

        internal FileOperations(UnixOperations unixOperations)
        {
            _unixOperations = unixOperations;
        }

        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }

        public virtual void Write(string path, string content, Action<UnixStream> validator = null)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                File.WriteAllText(path, content);
            }
            else
            {
                _unixOperations.WriteAllText(path, content, validator);
            }
        }

        public virtual void Append(string path, string mainContent, string additionalContent)
        {
            File.AppendAllText(path, mainContent);
            if (additionalContent != null)
                File.AppendAllText(path, additionalContent);
        }

        public virtual void WriteAllBytes(string path, byte[] bytes)
        {
            File.WriteAllBytes(path, bytes);
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

        public virtual byte[] ReadAllBytes(string path) => File.ReadAllBytes(path);

        public virtual Stream CreateTempFile(string filePath)
        {
            var absolutePath = Path.Combine(TempUtil.GetTempPath(), filePath);

            return Create(absolutePath);
        }

        public virtual Stream Create(string filePath, FileAccessPermissions fileAccessPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite)
        {
            var absolutePath = Path.GetFullPath(filePath);

            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
                File.Create(absolutePath) :
                _unixOperations.CreateFileWithPermissions(absolutePath, fileAccessPermissions);
        }

        public virtual void CopyFile(string src, string dst)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                File.Delete(dst);
                File.Copy(src, dst);
                return;
            }

            var fileInfo = new UnixFileInfo(src);
            using (var srcStream = fileInfo.OpenRead())
            {
                if (!IsFileStreamSafe(srcStream))
                    throw new SecurityException($"File {src} is not safe to read.");

                File.Delete(dst);
                using (var dstStream = Create(dst))
                {
                    srcStream.CopyTo(dstStream);
                }
            }
        }

        private bool IsFileStreamSafe(UnixStream stream)
        {
            if (_unixOperations.CheckFileHasAnyOfPermissions(stream.FileAccessPermissions, NotSafePermissions))
                return false;

            if (stream.OwnerUser.UserId != _unixOperations.GetCurrentUserId())
                return false;

            return true;
        }

        public virtual void SetLastWriteTimeUtc(string path, DateTime lastWriteTimeUtc)
        {
            File.SetLastWriteTimeUtc(path, lastWriteTimeUtc);
        }

        public virtual FileInformation GetFileInfo(string path)
        {
            var fileInfo = new FileInfo(path);
            return new FileInformation(fileInfo);
        }
    }
}
