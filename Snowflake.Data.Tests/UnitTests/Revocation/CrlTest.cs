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
            Assert.IsFalse(crl.NeedsFreshCrl(now));
            Assert.AreEqual(TimeSpan.FromDays(10), crl.CrlCacheValidityTime);
        }

        [Test]
        public void TestCrlParseWithCustomValidityTime()
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable(CrlParser.CrlValidityTimeEnvName))
                .Returns("1");
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = new DateTime(2025, 7, 25, 16, 57, 0, DateTimeKind.Utc);

            // act
            var crl = crlParser.Parse(crlBytes, now);

            // assert
            Assert.AreEqual(TimeSpan.FromDays(1), crl.CrlCacheValidityTime);
        }

        [Test]
        [TestCase("2025-07-25T16:57:00.0000000Z", false)]
        [TestCase("2025-08-02T16:57:00.0000000Z", true)]
        public void TestIfFreshCrlIsNeededDependingOnNextUpdate(string nowString, bool expectedIsFreshCrlNeeded)
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = DateTimeOffset.Parse(nowString).UtcDateTime;
            var crl = crlParser.Parse(crlBytes, now);

            // act
            var isFreshCrlNeeded = crl.NeedsFreshCrl(now);

            // assert
            Assert.AreEqual(expectedIsFreshCrlNeeded, isFreshCrlNeeded);
        }

        [Test]
        public void TestFreshCrlIsNeededDependingOnValidityTime()
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = new DateTime(2025, 7, 25, 16, 57, 0, DateTimeKind.Utc);
            var crl = crlParser.Parse(crlBytes, now);
            Assert.False(crl.NeedsFreshCrl(now));
            var justAfterValidityTime = now.Add(crl.CrlCacheValidityTime).AddSeconds(1);

            // act
            var isFreshCrlNeeded = crl.NeedsFreshCrl(justAfterValidityTime);

            // assert
            Assert.True(isFreshCrlNeeded);
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
