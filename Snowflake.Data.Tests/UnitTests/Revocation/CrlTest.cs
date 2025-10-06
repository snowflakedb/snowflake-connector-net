using System;
using System.IO;
using Moq;
using NUnit.Framework;
using Org.BouncyCastle.Math;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlTest : RevocationTests
    {
        [Test]
        public void TestCrlParse()
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = new DateTime(2025, 7, 25, 16, 57, 0, DateTimeKind.Utc);
            var expectedCrlDistributionPoints = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };

            // act
            var crl = crlParser.Parse(crlBytes, now);

            // assert
            Assert.AreEqual(now, crl.DownloadTime);
            Assert.AreEqual(DateTimeKind.Utc, crl.ThisUpdate.Kind);
            Assert.AreEqual(DigiCertThisUpdateString, crl.ThisUpdate.ToUniversalTime().ToString("o"));
            Assert.AreEqual(DateTimeKind.Utc, crl.NextUpdate?.Kind);
            Assert.AreEqual(DigiCertNextUpdateString, crl.NextUpdate?.ToUniversalTime().ToString("o"));
            Assert.AreEqual(DigiCertIssuer, crl.IssuerName);
            Assert.That(crl.IssuerDistributionPoints, Is.EquivalentTo(expectedCrlDistributionPoints));
            Assert.That(crl.RevokedCertificates, Does.Contain(DigiCertRevokedCertSerialNumber));
            Assert.IsTrue(crl.IsRevoked(DigiCertRevokedCertSerialNumber));
            Assert.IsFalse(crl.RevokedCertificates.Contains(DigiCertUnrevokedCertSerialNumber));
            Assert.IsFalse(crl.IsRevoked(DigiCertUnrevokedCertSerialNumber));
            Assert.IsFalse(crl.IsExpiredOrStale(now, TimeSpan.FromDays(1)));
        }

        [Test]
        [TestCase("2025-07-25T16:57:00.0000000Z", false)]
        [TestCase("2025-08-02T16:57:00.0000000Z", true)]
        public void TestCrlExpiredDependingOnNextUpdate(string nowString, bool expectedIsExpired)
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = DateTimeOffset.Parse(nowString).UtcDateTime;
            var crl = crlParser.Parse(crlBytes, now);

            // act
            var isExpired = crl.IsExpiredOrStale(now, TimeSpan.FromDays(365));

            // assert
            Assert.AreEqual(expectedIsExpired, isExpired);
        }

        [Test]
        public void TestCrlParserDefaultValidityTime()
        {
            // arrange
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var validityTime = crlParser.GetCacheValidityTime();

            // assert
            Assert.AreEqual(TimeSpan.FromDays(1), validityTime);
        }

        [Test]
        public void TestCrlParserCustomValidityTime()
        {
            // arrange
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable(CrlParser.CrlValidityTimeEnvName))
                .Returns("7");
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var validityTime = crlParser.GetCacheValidityTime();

            // assert
            Assert.AreEqual(TimeSpan.FromDays(7), validityTime);
        }

        [Test]
        public void TestCrlIsStaleAfterValidityTime()
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = new DateTime(2025, 7, 25, 16, 57, 0, DateTimeKind.Utc);
            var crl = crlParser.Parse(crlBytes, now);
            var cacheValidityTime = TimeSpan.FromDays(1);
            Assert.False(crl.IsExpiredOrStale(now, cacheValidityTime));
            var justAfterValidityTime = now.Add(cacheValidityTime).AddSeconds(1);

            // act
            var isExpiredOrStale = crl.IsExpiredOrStale(justAfterValidityTime, cacheValidityTime);

            // assert
            Assert.True(isExpiredOrStale);
        }

        [Test]
        [TestCase("127", "7F")]
        [TestCase("32768", "008000")]
        public void TestConvertBigIntegerToHexString(string stringValue, string expectedHexString)
        {
            // arrange
            var bigInt = new BigInteger(stringValue);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var hexString = crlParser.ConvertToHexadecimalString(bigInt);

            // assert
            Assert.AreEqual(expectedHexString, hexString);
        }
    }
}
