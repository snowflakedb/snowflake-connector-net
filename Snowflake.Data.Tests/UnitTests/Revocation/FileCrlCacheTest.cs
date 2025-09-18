using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Web;
using Mono.Unix;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Moq;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
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

        [Test]
        public void TestGetAndSetCacheOperations()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance);
            try
            {
                // act
                var crlReadFromNotExistingFile = cache.Get(CrlUrl);

                // assert
                Assert.IsNull(crlReadFromNotExistingFile);

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

        [Test]
        public void TestSetOverridesExistingFile()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance);
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

        [Test]
        [Platform(Exclude = "Win")]
        public void TestDontReadFromInsecureFile()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance);
            try
            {
                cache.Set(CrlUrl, crl);
                var fileName = Directory.GetFiles(tempDirectory)[0];
                var tooBroadPermissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.OtherWrite;
                UnixOperations.Instance.ChangePermissions(fileName, tooBroadPermissions);

                // act
                var crlReadFromTooPermissiveFile = cache.Get(CrlUrl);

                // assert
                Assert.IsNull(crlReadFromTooPermissiveFile);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestSetSecurePermissionsWhenSavingCrlCache()
        {
            // arrange
            var crl = CreateCrl();
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var cacheConfig = CreateFileCrlCacheConfig(tempDirectory);
            var cache = new FileCrlCache(cacheConfig, s_crlParser, FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance);
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
                Assert.AreEqual(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite, filePermissions);
                var dirPermissions = UnixOperations.Instance.GetDirPermissions(tempDirectory);
                Assert.AreEqual(FileAccessPermissions.UserReadWriteExecute, dirPermissions);
            }
            finally
            {
                DirectoryOperations.Instance.Delete(tempDirectory, true);
            }
        }

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
        public void TestWithMocksWindowsGetCrlForNotExistingFile()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(false);

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.IsNull(crlRead);
            unixOperations.VerifyNoOtherCalls();
        }

        [Test]
        public void TestWithMocksWindowsGetCrlForReadingError()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, true, 0, 0);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            fileOperations
                .Setup(f => f.ReadAllBytes(fileName))
                .Throws(() => new Exception("Failed to read crl file"));

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.IsNull(crlRead);
            unixOperations.VerifyNoOtherCalls();
        }

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);

            // act
            cache.Set(CrlUrl, crl);

            // assert
            directoryOperations.Verify(d => d.CreateDirectory(tempDirectory));
            fileOperations.Verify(f => f.WriteAllBytes(fileName, crl.GetEncoded()), Times.Once());
            fileOperations.Verify(f => f.SetLastWriteTimeUtc(fileName, s_downloadTime), Times.Once());
            unixOperations.VerifyNoOtherCalls();
        }

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
        public void TestWithMocksUnixGetCrlFailsForReadingError()
        {
            // arrange
            var tempDirectory = Path.Combine(Path.GetTempPath(), $"crl_cache_{Path.GetRandomFileName()}");
            var fileName = Path.Combine(tempDirectory, HttpUtility.UrlEncode(CrlUrl, Encoding.UTF8));
            var cacheConfig = new FileCrlCacheConfig(tempDirectory, false, UnixUserId, UnixGroupId);
            var fileOperations = new Mock<FileOperations>();
            var unixOperations = new Mock<UnixOperations>();
            var directoryOperations = new Mock<DirectoryOperations>();
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
            fileOperations
                .Setup(f => f.Exists(fileName))
                .Returns(true);
            unixOperations
                .Setup(u => u.ReadAllBytes(fileName, It.IsAny<Action<UnixStream>>()))
                .Throws(() => new Exception("Failed to read crl file"));

            // act
            var crlRead = cache.Get(CrlUrl);

            // assert
            Assert.IsNull(crlRead);
        }

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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

        [Test]
        [TestCase(1, 0, 0, 0)]
        [TestCase(0, 1, 0, 0)]
        [TestCase(0, 0, 1, 0)]
        [TestCase(0, 0, 1, 1)]
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
            var cache = new FileCrlCache(cacheConfig, s_crlParser, fileOperations.Object, unixOperations.Object, directoryOperations.Object);
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
            Assert.AreEqual(expected.DownloadTime, actual.DownloadTime);
            Assert.AreEqual(expected.ThisUpdate, actual.ThisUpdate);
            Assert.AreEqual(expected.NextUpdate, actual.NextUpdate);
            Assert.AreEqual(expected.IssuerName, actual.IssuerName);
            CollectionAssert.AreEqual(expected.IssuerDistributionPoints, actual.IssuerDistributionPoints);
            CollectionAssert.AreEqual(expected.RevokedCertificates, actual.RevokedCertificates);
            Assert.AreEqual(expected.CrlCacheValidityTime, actual.CrlCacheValidityTime);
        }
    }
}
