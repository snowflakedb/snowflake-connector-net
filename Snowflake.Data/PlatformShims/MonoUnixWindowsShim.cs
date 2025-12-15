// This file is compiled ONLY when WINDOWS_BUILD is defined
// It provides stub implementations of Mono.Unix types that throw PlatformNotSupportedException
// since Unix-specific operations should never be called on Windows (protected by runtime guards)

#if WINDOWS_BUILD

using System;
using System.IO;

namespace Mono.Unix
{
    // These values are NEVER used at runtime on Windows
    // The real Mono.Unix library is only used on Linux/macOS
    // Using -1 (stub value) to make it clear these should never be evaluated at runtime
    [Flags]
    public enum FileAccessPermissions
    {
        None = -1,
        OtherExecute = -1,
        OtherWrite = -1,
        OtherRead = -1,
        OtherReadWriteExecute = -1,
        GroupExecute = -1,
        GroupWrite = -1,
        GroupRead = -1,
        GroupReadWriteExecute = -1,
        UserExecute = -1,
        UserWrite = -1,
        UserRead = -1,
        UserReadWriteExecute = -1,
        AllPermissions = -1,
    }

    internal class UnixStream : Stream
    {
        private const string ErrorMessage = "Unix file operations are not supported on Windows";

        public UnixUserInfo OwnerUser => throw new PlatformNotSupportedException(ErrorMessage);
        public UnixGroupInfo OwnerGroup => throw new PlatformNotSupportedException(ErrorMessage);
        public long OwnerUserId => throw new PlatformNotSupportedException(ErrorMessage);
        public long OwnerGroupId => throw new PlatformNotSupportedException(ErrorMessage);
        public FileAccessPermissions FileAccessPermissions => throw new PlatformNotSupportedException(ErrorMessage);

        public override bool CanRead => throw new PlatformNotSupportedException(ErrorMessage);
        public override bool CanSeek => throw new PlatformNotSupportedException(ErrorMessage);
        public override bool CanWrite => throw new PlatformNotSupportedException(ErrorMessage);
        public override long Length => throw new PlatformNotSupportedException(ErrorMessage);
        public override long Position
        {
            get => throw new PlatformNotSupportedException(ErrorMessage);
            set => throw new PlatformNotSupportedException(ErrorMessage);
        }

        public override void Flush() => throw new PlatformNotSupportedException(ErrorMessage);
        public override int Read(byte[] buffer, int offset, int count) => throw new PlatformNotSupportedException(ErrorMessage);
        public override long Seek(long offset, SeekOrigin origin) => throw new PlatformNotSupportedException(ErrorMessage);
        public override void SetLength(long value) => throw new PlatformNotSupportedException(ErrorMessage);
        public override void Write(byte[] buffer, int offset, int count) => throw new PlatformNotSupportedException(ErrorMessage);
    }

    internal class UnixFileInfo
    {
        private const string ErrorMessage = "Unix file operations are not supported on Windows";

        public UnixFileInfo(string path)
        {
            throw new PlatformNotSupportedException(ErrorMessage);
        }

        public string FullName => throw new PlatformNotSupportedException(ErrorMessage);
        public bool Exists => throw new PlatformNotSupportedException(ErrorMessage);
        public FileAccessPermissions FileAccessPermissions => throw new PlatformNotSupportedException(ErrorMessage);
        public long OwnerUserId => throw new PlatformNotSupportedException(ErrorMessage);
        public UnixUserInfo OwnerUser => throw new PlatformNotSupportedException(ErrorMessage);
        public long Length => throw new PlatformNotSupportedException(ErrorMessage);

        public Stream Create(FileAccessPermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);

        public UnixStream OpenRead()
            => throw new PlatformNotSupportedException(ErrorMessage);

        public UnixStream Open(FileMode mode, FileAccess access, Native.FilePermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);
    }

    internal class UnixDirectoryInfo
    {
        private const string ErrorMessage = "Unix directory operations are not supported on Windows";

        public UnixDirectoryInfo(string path)
        {
            throw new PlatformNotSupportedException(ErrorMessage);
        }

        public string FullName => throw new PlatformNotSupportedException(ErrorMessage);
        public bool Exists => throw new PlatformNotSupportedException(ErrorMessage);
        public FileAccessPermissions FileAccessPermissions => throw new PlatformNotSupportedException(ErrorMessage);
        public long OwnerUserId => throw new PlatformNotSupportedException(ErrorMessage);
        public UnixUserInfo OwnerUser => throw new PlatformNotSupportedException(ErrorMessage);

        public void Create(FileAccessPermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);
    }

    internal class UnixUserInfo
    {
        private const string ErrorMessage = "Unix user operations are not supported on Windows";

        public UnixUserInfo(long userId)
        {
            throw new PlatformNotSupportedException(ErrorMessage);
        }

        public long UserId => throw new PlatformNotSupportedException(ErrorMessage);
    }

    internal class UnixGroupInfo
    {
        private const string ErrorMessage = "Unix group operations are not supported on Windows";

        public UnixGroupInfo(long groupId)
        {
            throw new PlatformNotSupportedException(ErrorMessage);
        }

        public long GroupId => throw new PlatformNotSupportedException(ErrorMessage);
    }
}

namespace Mono.Unix.Native
{
    // These values are NEVER used at runtime on Windows - they're only passed to Syscall methods that throw
    // The real Mono.Unix.Native library is only used on Linux/macOS
    // Using -1 (stub value) to make it clear these should never be evaluated at runtime
    [Flags]
    internal enum FilePermissions
    {
        S_ISUID = -1,
        S_ISGID = -1,
        S_ISVTX = -1,
        S_IRUSR = -1,
        S_IWUSR = -1,
        S_IXUSR = -1,
        S_IRGRP = -1,
        S_IWGRP = -1,
        S_IXGRP = -1,
        S_IROTH = -1,
        S_IWOTH = -1,
        S_IXOTH = -1,

        S_IRWXU = -1,
        S_IRWXG = -1,
        S_IRWXO = -1,

        ACCESSPERMS = -1,
        ALLPERMS = -1,
        DEFFILEMODE = -1
    }

    internal static class Syscall
    {
        private const string ErrorMessage = "Unix syscalls are not supported on Windows";

        public static int mkdir(string path, FilePermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static int creat(string path, FilePermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long chown(string path, int userId, int groupId)
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long chmod(string path, FilePermissions permissions)
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long getuid()
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long getgid()
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long geteuid()
            => throw new PlatformNotSupportedException(ErrorMessage);

        public static long getegid()
            => throw new PlatformNotSupportedException(ErrorMessage);
    }
}

#endif
