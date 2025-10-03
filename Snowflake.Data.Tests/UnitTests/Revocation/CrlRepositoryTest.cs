using System;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlRepositoryTest
    {
        const string CrlUrlBase = "http://snowflakecomputing.com/crl1.crl";

        [Test]
        public void TestGetCrlFromMemory()
        {
            var crlRepository = new CrlRepository(useMemoryCache: true, useFileCache: false);
            var crlUrl = $"{CrlUrlBase}?test=memory";
            var now = DateTime.UtcNow;
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = "Test"
            };
            crlRepository.Set(crlUrl, crl);

            var retrieved = crlRepository.Get(crlUrl);

            Assert.NotNull(retrieved);
            Assert.AreEqual(crl.IssuerName, retrieved.IssuerName);
        }

        [Test]
        public void TestGetCrlFromFileCache()
        {
            var crlRepository = new CrlRepository(useMemoryCache: false, useFileCache: true);
            var crlUrl = $"{CrlUrlBase}?test=file";
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
            crlRepository.Set(crlUrl, crl);

            var retrieved = crlRepository.Get(crlUrl);

            Assert.NotNull(retrieved);
            Assert.AreEqual(issuerName, retrieved.IssuerName);
        }

        [Test]
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        public void TestConstructCaches(bool useMemoryCache, bool useFileCache)
        {
            var crlRepository = new CrlRepository(useMemoryCache, useFileCache);
            var crlUrl = $"{CrlUrlBase}?test={useMemoryCache}_{useFileCache}";
            var now = DateTime.UtcNow;
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = now,
                NextUpdate = now.AddDays(10),
                IssuerName = "Test"
            };

            crlRepository.Set(crlUrl, crl);
            var retrieved = crlRepository.Get(crlUrl);

            Assert.NotNull(retrieved);
            Assert.AreEqual(crl.IssuerName, retrieved.IssuerName);
        }
    }
}
