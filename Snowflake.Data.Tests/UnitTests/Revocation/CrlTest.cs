using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Moq;
using NUnit.Framework;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

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
            Assert.IsNotNull(crl.IssuerNameRawData);
            CollectionAssert.AreEqual(crl.BouncyCastleCrl.IssuerDN.GetEncoded(), crl.IssuerNameRawData);
            Assert.That(crl.IssuerDistributionPoints, Is.EquivalentTo(expectedCrlDistributionPoints));
            Assert.That(crl.RevokedCertificates, Does.Contain(DigiCertRevokedCertSerialNumber));
            Assert.IsTrue(crl.IsRevoked(DigiCertRevokedCertSerialNumber));
            Assert.IsFalse(crl.RevokedCertificates.Contains(DigiCertUnrevokedCertSerialNumber));
            Assert.IsFalse(crl.IsRevoked(DigiCertUnrevokedCertSerialNumber));
            Assert.IsFalse(crl.NeedsReplacement(now, TimeSpan.FromDays(1)));
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
            var isExpired = crl.NeedsReplacement(now, TimeSpan.FromDays(365));

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
            Assert.False(crl.NeedsReplacement(now, cacheValidityTime));
            var justAfterValidityTime = now.Add(cacheValidityTime).AddSeconds(1);

            // act
            var needsReplacement = crl.NeedsReplacement(justAfterValidityTime, cacheValidityTime);

            // assert
            Assert.True(needsReplacement);
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

        [Test]
        [TestCase("084E2808851C58174D0EF94B29571042", Description = "Uppercase - matches stored format")]
        [TestCase("084e2808851c58174d0ef94b29571042", Description = "Lowercase - case-insensitive match")]
        [TestCase("084E2808851c58174d0ef94B29571042", Description = "Mixed case - case-insensitive match")]
        public void TestIsRevokedIsCaseInsensitive(string serialNumber)
        {
            // arrange
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var now = new DateTime(2010, 04, 10, 16, 57, 0, DateTimeKind.Utc);
            var crl = crlParser.Parse(crlBytes, now);

            // act
            var isRevoked = crl.IsRevoked(serialNumber);

            // assert
            Assert.IsTrue(isRevoked);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestIsRevokedHandlesLeadingZeroBytePadding(bool serialHasHighBitSet)
        {
#if !NET8_0_OR_GREATER
            Assert.Pass();
        }
#else
            // DER encodes positive integers with a leading 0x00 when the high bit is set.
            // This test verifies CrlParser.ConvertToHexadecimalString produces a hex string
            // consistent with X509Certificate2.SerialNumber in both cases.

            // arrange
            var rootSubject = "CN=Root CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var certKeys = CertificateGenerator.GenerateKeysForCertAndItsParent();

            // Generate certs until we get one with/without the 0x00 prefix (depending on test case).
            // CreateSelfSigned uses a random 64-bit serial; roughly half will have the high bit set.
            X509Certificate2 certificate;
            var expectedBytesCount = serialHasHighBitSet ? 9 : 8;
            var counter = 0;
            do
            {
                certificate = BuildSelfSignedCertificate(counter);
                Assert.Less(counter++, 20, "Gave up waiting for desired serial number form");
            } while (certificate.SerialNumberBytes.Length != expectedBytesCount);
            // Expected value of number of loop iterations here = 2

            if (serialHasHighBitSet)
                Assert.AreEqual("00", certificate.SerialNumber[..2], "Serial with high bit set should have leading 0x00 in hex representation");

            // Build a CRL containing this serial as revoked
            var serialAsBigInt = new BigInteger(certificate.SerialNumber, 16);
            var crlGenerator = new X509V2CrlGenerator();
            crlGenerator.SetIssuerDN(new X509Name(rootSubject));
            var now = DateTime.UtcNow;
            crlGenerator.SetThisUpdate(now.AddDays(-1));
            crlGenerator.SetNextUpdate(now.AddDays(7));
            crlGenerator.AddCrlEntry(serialAsBigInt, now.AddDays(-1), CrlReason.KeyCompromise);
            var signatureFactory = new Asn1SignatureFactory(CertificateGenerator.SHA256WithRsaAlgorithm, certKeys[1].Private, new SecureRandom());
            var bouncyCrl = crlGenerator.Generate(signatureFactory);

            // Parse through production code path
            var environmentOperations = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperations.Object);
            var crl = crlParser.Create(bouncyCrl, now);

            var bouncyCastleHex = crlParser.ConvertToHexadecimalString(serialAsBigInt);
            Assert.IsTrue(crl.RevokedCertificates.Contains(bouncyCastleHex), "CRL should contain the revoked serial");

            // act
            var isRevoked = crl.IsRevoked(certificate.SerialNumber);

            // assert — both representations must match regardless of leading 0x00 presence
            Assert.AreEqual(bouncyCastleHex, certificate.SerialNumber, $"Hex mismatch: CRL has '{bouncyCastleHex}', cert has '{certificate.SerialNumber}'");
            Assert.IsTrue(isRevoked, "Certificate should be found in the revocation list");
        }

        private static X509Certificate2 BuildSelfSignedCertificate(int runNo)
        {
            var distinguishedName = new X500DistinguishedName($"CN=TestCert{runNo}, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland");
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(distinguishedName, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        }
#endif
    }
}
