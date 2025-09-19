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
        public void TestExtractCertificateDistributionPoints(CrlExtractionTestCase testCase)
        {
            // arrange
            var certificate = CertificateGenerator.GenerateSelfSignedCertificateWithDefaultSubject("other CA", DateTime.Now.AddYears(-1), DateTime.Now.AddYears(1), testCase.CrlDistributionPoints);
            var crlExtractor = new CertificateCrlDistributionPointsExtractor();

            // act
            var crlUrls = crlExtractor.Extract(certificate);

            // assert
            Assert.AreEqual(testCase.ExpectedCrlUrls.Length, crlUrls.Length);
            Assert.That(testCase.ExpectedCrlUrls, Is.EquivalentTo(crlUrls));
        }

        public static IEnumerable<CrlExtractionTestCase> CrlsTestCases()
        {
            yield return new CrlExtractionTestCase
            {
                CrlDistributionPoints = new[] { new[] { "http://snowflake.com/crl1.crl" }, new[] { "http://snowflake.com/crl2.crl" } },
                ExpectedCrlUrls = new[] { "http://snowflake.com/crl1.crl", "http://snowflake.com/crl2.crl" }
            };

            yield return new CrlExtractionTestCase
            {
                CrlDistributionPoints = new[] {
                    new[] { "http://snowflake.com/crl1.crl", "ftp://snowflake.com/crl1.crl" },
                    new[] { "ftp://snowflake.com/crl2.crl", "http://snowflake.com/crl2.crl" }
                },
                ExpectedCrlUrls = new[] { "http://snowflake.com/crl1.crl", "http://snowflake.com/crl2.crl" }
            };

            yield return new CrlExtractionTestCase
            {
                CrlDistributionPoints = new string[][] { },
                ExpectedCrlUrls = new string[] { }
            };
        }

        public class CrlExtractionTestCase
        {
            public string[][] CrlDistributionPoints { get; set; }
            public string[] ExpectedCrlUrls { get; set; }
        }
    }
}
