using System;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlCacheManagerTest
    {
        const string CrlUrl1 = "http://snowflakecomputing.com/crl1.crl";
        const string CrlUrl2 = "http://snowflakecomputing.com/crl2.crl";

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
        public void TestScheduledCleanupRemovesEvictedEntries()
        {
            // arrange
            var manager = CrlCacheManager.Build(
                inMemoryCacheEnabled: true,
                onDiskCacheEnabled: false,
                cleanupInterval: TimeSpan.FromMilliseconds(200),
                cacheValidityTime: TimeSpan.FromDays(1));

            var now = DateTime.UtcNow;
            var evictedCrl = new Crl
            {
                DownloadTime = now.AddDays(-2),
                ThisUpdate = now.AddDays(-2),
                NextUpdate = now.AddDays(10),
                IssuerName = "Test Issuer"
            };

            manager.Set(CrlUrl1, evictedCrl);
            Assert.NotNull(manager.Get(CrlUrl1));

            // act:
            Thread.Sleep(500);

            // assert
            Assert.Null(manager.Get(CrlUrl1));
        }
    }
}

