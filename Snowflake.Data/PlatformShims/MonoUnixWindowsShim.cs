// This file is compiled ONLY on Windows (when WINDOWS_BUILD is defined)
// It provides stub implementations of Mono.Unix types that throw PlatformNotSupportedException
// since Unix-specific operations should never be called on Windows (protected by runtime guards)

#if WINDOWS_BUILD

using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Mono.Unix
{
    [Flags]
    internal enum FileAccessPermissions
    {
        None = 0,
        OtherExecute = 1,
        OtherWrite = 2,
        OtherRead = 4,
        OtherReadWriteExecute = 7,
        GroupExecute = 8,
        GroupWrite = 16,
        GroupRead = 32,
        GroupReadWriteExecute = 56,
        UserExecute = 64,
        UserWrite = 128,
        UserRead = 256,
        UserReadWrite = 384,
        UserReadWriteExecute = 448,
        AllPermissions = 511,
        DefaultPermissions = 420
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
    [Flags]
    internal enum FilePermissions
    {
        S_ISUID = 0x0800,
        S_ISGID = 0x0400,
        S_ISVTX = 0x0200,
        S_IRUSR = 0x0100,
        S_IWUSR = 0x0080,
        S_IXUSR = 0x0040,
        S_IRGRP = 0x0020,
        S_IWGRP = 0x0010,
        S_IXGRP = 0x0008,
        S_IROTH = 0x0004,
        S_IWOTH = 0x0002,
        S_IXOTH = 0x0001,

        S_IRWXU = S_IRUSR | S_IWUSR | S_IXUSR,
        S_IRWXG = S_IRGRP | S_IWGRP | S_IXGRP,
        S_IRWXO = S_IROTH | S_IWOTH | S_IXOTH,

        ACCESSPERMS = S_IRWXU | S_IRWXG | S_IRWXO,
        ALLPERMS = S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO,
        DEFFILEMODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH
    }

    internal static class Syscall
    {
        private const string ErrorMessage = "Unix syscalls are not supported on Windows";

        public static int mkdir(string path, FilePermissions permissions)
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

