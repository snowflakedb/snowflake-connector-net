using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CertificateCrlDistributionPointsExtractorTest
    {
        [Test]
        [TestCaseSource(nameof(CrlsTestCases))]
        public void TestExtractCertificateDistributionPoints(string[] expectedCrlUrls)
        {
            // arrange
            var certificate = CertificateGenerator.GenerateSelfSignedCertificateWithDefaultSubject("other CA", DateTime.Now.AddYears(-1), DateTime.Now.AddYears(1), expectedCrlUrls);
            var crlExtractor = new CertificateCrlDistributionPointsExtractor();

            // act
            var crlUrls = crlExtractor.Extract(certificate);

            // assert
            Assert.AreEqual(expectedCrlUrls.Length, crlUrls.Length);
            Assert.That(expectedCrlUrls, Is.EquivalentTo(crlUrls));
        }

        static IEnumerable<string[]> CrlsTestCases()
        {
            yield return new[] { "http://snowflake.com/crl1.crl", "http://snowflake.com/crl2.crl" };
            yield return new string[] { };
        }
    }
}
