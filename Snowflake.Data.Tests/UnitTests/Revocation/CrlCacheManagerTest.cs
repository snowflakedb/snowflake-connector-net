using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Web;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlCacheManagerTest
    {
        const string CrlUrl1 = "http://snowflakecomputing.com/crl1.crl";
        const string CrlUrl2 = "http://snowflakecomputing.com/crl2.crl";

        [SetUp]
        public void SetUp()
        {
            var cacheDir = GetCrlCacheDirectory();
            if (Directory.Exists(cacheDir))
            {
                try
                {
                    foreach (var file in Directory.GetFiles(cacheDir))
                    {
                        File.Delete(file);
                    }
                }
                catch
                {
                    // Ignore errors if files are already deleted
                }
            }
        }

        [Test]
        public void TestGetFromMemoryCacheOnly()
        {
            // arrange: manager with only memory cache
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: false,
                cleanupInterval: TimeSpan.FromDays(1),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var issuerName = "CN=Test CA,O=Test Org";
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = issuerName
            };

            // act
            manager.Set(CrlUrl1, crl);
            var retrieved = manager.Get(CrlUrl1);

            // assert
            Assert.NotNull(retrieved);
            Assert.AreEqual(issuerName, retrieved.IssuerName);
        }

        [Test]
        public void TestGetFromDiskCacheOnly()
        {
            // arrange: manager with only disk cache
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: false,
                onDiskCacheEnabled: true,
                cleanupInterval: TimeSpan.FromDays(1),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var issuerName = "CN=Test CA,O=Test Org";
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = issuerName,
                BouncyCastleCrl = CertificateGenerator.GenerateCrl(issuerName, now, now.AddDays(10), now)
            };

            // act
            manager.Set(CrlUrl1, crl);
            var retrieved = manager.Get(CrlUrl1);

            // assert
            Assert.NotNull(retrieved);
            Assert.AreEqual(issuerName, retrieved.IssuerName);
        }


        [Test]
        public void TestGetReturnsNullForMissingEntry()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: true,
                cleanupInterval: TimeSpan.FromDays(1),
                cacheValidityTime: TimeSpan.FromDays(1));

            // act
            var result = manager.Get("http://nonexistent.com/crl.crl");

            // assert
            Assert.Null(result);
        }

        [Test]
        public void TestScheduledCleanupRemovesExpiredEntries()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: false,
                cleanupInterval: TimeSpan.FromMilliseconds(200),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var expiredCrl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(-1),
                IssuerName = "Test Issuer"
            };
            var validCrl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = "Test Issuer 2"
            };

            manager.Set(CrlUrl1, expiredCrl);
            manager.Set(CrlUrl2, validCrl);
            Assert.NotNull(manager.Get(CrlUrl1));
            Assert.NotNull(manager.Get(CrlUrl2));

            // act
            Thread.Sleep(500);

            // assert
            Assert.Null(manager.Get(CrlUrl1));
            Assert.NotNull(manager.Get(CrlUrl2));
        }

        [Test]
        public void TestScheduledCleanupRemovesStaleEntries()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: false,
                cleanupInterval: TimeSpan.FromMilliseconds(200),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var staleCrl = new Crl
            {
                DownloadTime = now.AddDays(-2),
                ThisUpdate = now.AddDays(-2),
                NextUpdate = now.AddDays(10),
                IssuerName = "Test Issuer"
            };

            manager.Set(CrlUrl1, staleCrl);
            Assert.NotNull(manager.Get(CrlUrl1));

            // act:
            Thread.Sleep(500);

            // assert
            Assert.Null(manager.Get(CrlUrl1));
        }

        [Test]
        public void TestCleanupKeepsValidEntries()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: false,
                cleanupInterval: TimeSpan.FromMilliseconds(200),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var validCrl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = "Test Issuer"
            };

            manager.Set(CrlUrl1, validCrl);
            Assert.NotNull(manager.Get(CrlUrl1));

            // act
            Thread.Sleep(500);

            // assert
            Assert.NotNull(manager.Get(CrlUrl1));
        }

        [Test]
        public void TestGetPromotesFromFileCacheToMemoryCache()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: true,
                cleanupInterval: TimeSpan.FromDays(1),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var issuerName = "CN=Test CA,O=Test Org";
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = issuerName,
                BouncyCastleCrl = CertificateGenerator.GenerateCrl(issuerName, now, now.AddDays(10), now)
            };


            manager.Set(CrlUrl1, crl);

            // Build a new manager instance with same file cache but new memory cache
            var manager2 = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: true,
                cleanupInterval: TimeSpan.FromDays(1),
                cacheValidityTime: TimeSpan.FromDays(1));

            // act
            var result1 = manager2.Get(CrlUrl1);
            Assert.NotNull(result1);

            // Delete the file from disk to verify second get comes from memory cache
            var filePath = GetCrlCacheFilePath(CrlUrl1);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            var result2 = manager2.Get(CrlUrl1);

            // assert
            Assert.NotNull(result2, "CRL should be found in memory cache even after file deletion");
            Assert.AreEqual(issuerName, result2.IssuerName);
        }

        private static string GetCrlCacheDirectory()
        {
            var homeDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return Path.Combine(homeDirectory, "AppData", "Local", "Snowflake", "Caches", "crls");
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return Path.Combine(homeDirectory, "Library", "Caches", "Snowflake", "crls");
            }
            else
            {
                return Path.Combine(homeDirectory, ".cache", "snowflake", "crls");
            }
        }

        private static string GetCrlCacheFilePath(string crlUrl)
        {
            var cacheDir = GetCrlCacheDirectory();
            var encodedUrl = HttpUtility.UrlEncode(crlUrl, Encoding.UTF8);
            return Path.Combine(cacheDir, encodedUrl);
        }
    }
}

