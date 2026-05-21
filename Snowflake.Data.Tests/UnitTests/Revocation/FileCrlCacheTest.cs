using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Web;
using Mono.Unix;
using Xunit;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Moq;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{

    public class FileCrlCacheTest : RevocationTests
    {
        const string CrlUrl = "http://snowflakecomputing.com/crl1.crl";
        const string IssuerCa = "CN=root CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
        const long UnixUserId = 5;
        const long UnixGroupId = 6;
        private static readonly DateTime s_thisUpdate = new(2025, 8, 10, 0, 0, 0, DateTimeKind.Utc);
        private static readonly DateTime s_nextUpdate = new(2026, 8, 10, 0, 0, 0, DateTimeKind.Utc);
        private static readonly DateTime s_revocationTime = s_thisUpdate;
        private static readonly DateTime s_downloadTime = new(2025, 8, 15, 8, 21, 33, DateTimeKind.Utc);
        private static readonly CrlParser s_crlParser = new(TimeSpan.FromDays(10));
        private static readonly TimeSpan s_removalDelay = TimeSpan.FromDays(7);

        [SFFact]
        public void TestGetAndSetCacheOperations()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance, s_removalDelay);
            try
            {
                // act
                var crlReadFromNotExistingFile = cache.Get(CrlUrl);

                // assert
                Assert.Null(crlReadFromNotExistingFile);

                // act
                cache.Set(CrlUrl, crl);
                var crlReadFromExistingFile = cache.Get(CrlUrl);

                // assert
                AssertCrlsAreEqual(crl, crlReadFromExistingFile);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact]
        public void TestSetOverridesExistingFile()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance, s_removalDelay);
            try
            {
                DirectoryOperations.Instance.CreateDirectory(tempDirectory);
                FileOperations.Instance.Write(fileName, "old content", cache.ValidateFileNotWritableByOthers);

                // act
                cache.Set(CrlUrl, crl);
                var crlReadFromExistingFile = cache.Get(CrlUrl);

                // assert
                AssertCrlsAreEqual(crl, crlReadFromExistingFile);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestDontReadFromInsecureFile()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance, s_removalDelay);
            try
            {
                cache.Set(CrlUrl, crl);
                var fileName = Directory.GetFiles(tempDirectory)[0];
                var tooBroadPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.OtherWrite;
                UnixOperations.Instance.ChangePermissions(fileName, tooBroadPermissions);

                // act
                var crlReadFromTooPermissiveFile = cache.Get(CrlUrl);

                // assert
                Assert.Null(crlReadFromTooPermissiveFile);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestSetSecurePermissionsWhenSavingCrlCache()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance, s_removalDelay);
            try
            {
                cache.Set(CrlUrl, crl);
                var fileName = Directory.GetFiles(tempDirectory)[0];
                var tooBroadFilePermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.OtherWrite;
                UnixOperations.Instance.ChangePermissions(fileName, tooBroadFilePermissions);
                var tooBroadDirPermissions = FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherWrite;
                UnixOperations.Instance.ChangePermissions(tempDirectory, tooBroadDirPermissions);

                // act
                cache.Set(CrlUrl, crl);
                var crlRead = cache.Get(CrlUrl);

                // assert
                AssertCrlsAreEqual(crl, crlRead);
                var filePermissions = UnixOperations.Instance.GetFilePermissions(fileName);
                Assert.Equal(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite, filePermissions);
                var dirPermissions = UnixOperations.Instance.GetDirPermissions(tempDirectory);
                Assert.Equal(FileAccessPermissions.UserReadWriteExecute, dirPermissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [SFFact]
        public void TestWithMocksWindowsGetCrl()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            fileOperations
                .Setup(f => f.ReadAllBytes(fileName))
                .Returns(crl.GetEncoded());
            fileOperations
                .Setup(f => f.GetFileInfo(fileName))
                .Returns(new FileInformation { Exists = true, LastWriteTimeUtc = s_downloadTime });

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            AssertCrlsAreEqual(crl, crlRead);
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestWithMocksWindowsGetCrlForNotExistingFile()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(false);

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.Null(crlRead);
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestWithMocksWindowsGetCrlForReadingError()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            fileOperations
                .Setup(f => f.ReadAllBytes(fileName))
                .Throws(() => new Exception("Failed to read crl file"));

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.Null(crlRead);
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestWithMocksWindowsSaveCrl()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);

            // act
            cache.Set(CrlUrl, crl);

            // assert
            directoryOperations.Verify(d => d.CreateDirectory(tempDirectory));
            fileOperations.Verify(f => f.WriteAllBytes(fileName, crl.GetEncoded()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestWithMocksWindowsSaveCrlError()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.WriteAllBytes(fileName, crl.GetEncoded()))
                .Throws(new Exception("Failed to write crl file"));

            // act
            cache.Set(CrlUrl, crl);

            // assert
            directoryOperations.Verify(d => d.CreateDirectory(tempDirectory));
            fileOperations.Verify(f => f.WriteAllBytes(fileName, crl.GetEncoded()), Times.Once());
            fileOperations.VerifyNoOtherCalls();
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        [Platform("Win")]
        public void TestWithMocksWindowsSaveCrlWithDirectoryCreation()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            directoryOperations
                .Setup(d => d.Exists(tempDirectory))
                .Returns(false);

            // act
            cache.Set(CrlUrl, crl);

            // assert
            directoryOperations.Verify(d => d.Exists(tempDirectory), Times.Once());
            directoryOperations.Verify(d => d.CreateDirectory(tempDirectory), Times.Once());
            fileOperations.Verify(f => f.WriteAllBytes(fileName, crl.GetEncoded()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestWithMocksUnixGetCrl()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            unixOperations
                .Setup(u => u.ReadAllBytes(fileName, It.IsAny<Action<UnixStream>>()))
                .Returns(crl.GetEncoded());
            fileOperations
                .Setup(f => f.GetFileInfo(fileName))
                .Returns(new FileInformation { Exists = true, LastWriteTimeUtc = s_downloadTime });

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            AssertCrlsAreEqual(crl, crlRead);
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestWithMocksUnixGetCrlFailsForReadingError()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            unixOperations
                .Setup(u => u.ReadAllBytes(fileName, It.IsAny<Action<UnixStream>>()))
                .Throws(() => new Exception("Failed to read crl file"));

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.Null(crlRead);
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestWithMocksUnixSaveCrl()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            unixOperations
                .Setup(u => u.GetDirectoryInfo(tempDirectory))
                .Returns(new DirectoryUnixInformation(tempDirectory, true, FileAccessPermissions.UserReadWriteExecute, UnixUserId));
            unixOperations
                .Setup(u => u.GetFileInfo(fileName))
                .Returns(new FileUnixInformation(fileName, true, FileAccessPermissions.UserReadWriteExecute, UnixUserId));

            // act
            cache.Set(CrlUrl, crl);

            // assert
            unixOperations.Verify(u => u.GetDirectoryInfo(tempDirectory), Times.Once());
            unixOperations.Verify(u => u.GetFileInfo(fileName), Times.Once());
            unixOperations.Verify(u => u.WriteAllBytes(fileName, crl.GetEncoded(), It.IsAny<Action<UnixStream>>()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestWithMocksUnixSaveCrlWithDirectoryCreation()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            unixOperations
                .Setup(u => u.GetDirectoryInfo(tempDirectory))
                .Returns(new DirectoryUnixInformation(tempDirectory, false, FileAccessPermissions.AllPermissions, 0));
            unixOperations
                .Setup(u => u.GetFileInfo(fileName))
                .Returns(new FileUnixInformation(fileName, false, FileAccessPermissions.AllPermissions, 0));

            // act
            cache.Set(CrlUrl, crl);

            // assert
            unixOperations.Verify(u => u.GetDirectoryInfo(tempDirectory), Times.Once());
            directoryOperations.Verify(d => d.CreateDirectory(tempDirectory), Times.Once());
            unixOperations.Verify(u => u.GetFileInfo(fileName), Times.Once());
            unixOperations.Verify(u => u.WriteAllBytes(fileName, crl.GetEncoded(), It.IsAny<Action<UnixStream>>()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        public void TestWithMocksUnixSaveCrlShouldFixIncorrectPermissions()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            unixOperations
                .Setup(u => u.GetDirectoryInfo(tempDirectory))
                .Returns(new DirectoryUnixInformation(tempDirectory, true, FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherWrite, UnixUserId + 100));
            unixOperations
                .Setup(u => u.GetFileInfo(fileName))
                .Returns(new FileUnixInformation(fileName, true, FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherWrite, UnixUserId + 100));

            // act
            cache.Set(CrlUrl, crl);

            // assert
            unixOperations.Verify(u => u.GetDirectoryInfo(tempDirectory), Times.Once());
            unixOperations.Verify(u => u.ChangeOwner(tempDirectory, (int)UnixUserId, (int)UnixGroupId), Times.Once());
            unixOperations.Verify(u => u.ChangePermissions(tempDirectory, FileAccessPermissions.UserReadWriteExecute), Times.Once());
            unixOperations.Verify(u => u.GetFileInfo(fileName), Times.Once());
            unixOperations.Verify(u => u.ChangeOwner(fileName, (int)UnixUserId, (int)UnixGroupId), Times.Once());
            unixOperations.Verify(u => u.ChangePermissions(fileName, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite), Times.Once());
            unixOperations.Verify(u => u.WriteAllBytes(fileName, crl.GetEncoded(), It.IsAny<Action<UnixStream>>()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [SFFact]
        [Platform(Exclude = "Win")]
        [InlineData(1, 0, 0, 0)]
        [InlineData(0, 1, 0, 0)]
        [InlineData(0, 0, 1, 0)]
        [InlineData(0, 0, 1, 1)]
        public void TestWithMocksUnixCrlShouldNotBeSavedIfFailedToSetSecurePermissions(long changeDirOwnerResult, long changeDirPermissionsResult, long changeFileOwnerResult, long changeFilePermissionsResult)
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object, s_removalDelay);
            unixOperations
                .Setup(u => u.GetDirectoryInfo(tempDirectory))
                .Returns(new DirectoryUnixInformation(tempDirectory, true, FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherWrite, UnixUserId + 100));
            unixOperations
                .Setup(u => u.GetFileInfo(fileName))
                .Returns(new FileUnixInformation(fileName, true, FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherWrite, UnixUserId + 100));
            unixOperations
                .Setup(u => u.ChangeOwner(tempDirectory, (int)UnixUserId, (int)UnixGroupId))
                .Returns(changeDirOwnerResult);
            unixOperations
                .Setup(u => u.ChangePermissions(tempDirectory, FileAccessPermissions.UserReadWriteExecute))
                .Returns(changeDirPermissionsResult);
            unixOperations
                .Setup(u => u.ChangeOwner(fileName, (int)UnixUserId, (int)UnixGroupId))
                .Returns(changeFileOwnerResult);
            unixOperations
                .Setup(u => u.ChangePermissions(fileName, FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite))
                .Returns(changeDirPermissionsResult);

            // act
            cache.Set(CrlUrl, crl);

            // assert
            unixOperations.Verify(u => u.GetDirectoryInfo(tempDirectory), Times.Once());
            unixOperations.Verify(u => u.WriteAllBytes(fileName, crl.GetEncoded(), It.IsAny<Action<UnixStream>>()), Times.Never());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Never());
        }

        private FileCrlCacheConfig CreateFileCrlCacheConfig(string tempDirectory)
        {
            bool isWindows;
            long unixUserId;
            long unixGroupId;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                isWindows = true;
                unixUserId = 0;
                unixGroupId = 0;
            }
            else
            {
                isWindows = false;
                unixUserId = UnixOperations.Instance.GetCurrentUserId();
                unixGroupId = UnixOperations.Instance.GetCurrentGroupId();
            }
            return new FileCrlCacheConfig(tempDirectory, isWindows, unixUserId, unixGroupId);
        }

        private Crl CreateCrl()
        {
            var x509Crl = CertificateGenerator.GenerateCrl(IssuerCa, s_thisUpdate, s_nextUpdate, s_revocationTime);
            return s_crlParser.Create(x509Crl, s_downloadTime);
        }

        private void AssertCrlsAreEqual(Crl expected, Crl actual)
        {
            Assert.Equal(expected.DownloadTime, actual.DownloadTime);
            Assert.Equal(expected.ThisUpdate, actual.ThisUpdate);
            Assert.Equal(expected.NextUpdate, actual.NextUpdate);
            Assert.Equal(expected.IssuerName, actual.IssuerName);
            CollectionAssert.Equal(expected.IssuerDistributionPoints, actual.IssuerDistributionPoints);
            CollectionAssert.Equal(expected.RevokedCertificates, actual.RevokedCertificates);
        }
    }
}
