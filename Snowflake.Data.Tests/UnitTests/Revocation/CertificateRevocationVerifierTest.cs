using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CertificateRevocationVerifierTest : RevocationTests
    {
        [Test]
        [TestCase("Enabled", false)]
        [TestCase("Advisory", true)]
        [TestCase("Disabled", true)]
        public void TestRevocationResultForErrorsBasedOnCheckMode(string checkMode, bool expectedResult)
        {
            // arrange
            var certRevocationCheckMode = (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode), checkMode, true);
            var certSubject = "CN=ShortLivedCert CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var rootSubject = "CN=root CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var certKeys = CertificateGenerator.GenerateKeysForCertAndItsParent();
            var certificate = CertificateGenerator.GenerateCertificate(certSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(300), null, certKeys[0]);
            var rootCertificate = CertificateGenerator.GenerateCertificate(rootSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(300), null, certKeys[1]);
            var chain = CertificateGenerator.CreateChain(new[] { certificate, rootCertificate });
            var config = GetHttpConfig(certRevocationCheckMode);
            var restRequester = new Mock<IRestRequester>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertificateRevocationStatus(certificate, chain); // there will be an error because we didn't configure crl distribution points

            // assert
            Assert.AreEqual(expectedResult, result);
        }

        [Test]
        public void TestVerifyCertificateAsUnrevoked()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.LoadFromFile(s_digiCertParentCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockByteResponseForGet(restRequester, DigiCertCrlUrl2, crlBytes);
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertUnrevoked, result);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenCouldNotDownloadCrl()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.LoadFromFile(s_digiCertParentCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockErrorResponseForGet(restRequester, DigiCertCrlUrl2, NotFoundHttpExceptionProvider);
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenOneOfCrlsIsNotParsable()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.LoadFromFile(s_digiCertParentCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var notParsableCrlBytes = Encoding.ASCII.GetBytes("not parsable crl");
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, notParsableCrlBytes);
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        public void TestFailWhenCrlSignatureNotMatchingParentKey()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.GenerateSelfSignedCertificate(DigiCertIssuer, DateTime.Now.AddYears(-1), DateTime.Now.AddYears(1), null);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockByteResponseForGet(restRequester, DigiCertCrlUrl2, crlBytes);
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, Core.Tools.TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestVerifyCrlSignatureForEllipticCurveCertificates(bool signCrlWithCaPrivateKey)
        {
            // arrange
            var certKeys = CertificateGenerator.GenerateEllipticKeysForCertAndItsParent();
            var certSubject = "CN=cert CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var rootSubject = "CN=root CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var signatureAlgorithm = CertificateGenerator.SHA256WithECDSA;
            var certificate = CertificateGenerator.GenerateCertificate(certSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(300), null, certKeys[0], false, signatureAlgorithm);
            var parentCertificate = CertificateGenerator.GenerateCertificate(rootSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(300), null, certKeys[1], true, signatureAlgorithm);
            var privateKeyToSignCrl = signCrlWithCaPrivateKey ? certKeys[1].Private : CertificateGenerator.GenerateECDSAKeyPair().Private;
            var bouncyCrl = CertificateGenerator.GenerateCrl(signatureAlgorithm, privateKeyToSignCrl, rootSubject, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(7), DateTime.UtcNow.AddDays(-1));
            var environmentOperation = new Mock<EnvironmentOperations>();
            var crlParser = new CrlParser(environmentOperation.Object);
            var crl = crlParser.Create(bouncyCrl, DateTime.UtcNow);
            var config = GetHttpConfig();
            var restRequester = new Mock<IRestRequester>();
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.IsCrlSignatureValid(crl, parentCertificate);

            // assert
            Assert.AreEqual(signCrlWithCaPrivateKey, result);
        }

        [Test]
        [TestCase(30, "ChainError")]
        [TestCase(3, "ChainUnrevoked")]
        public void TestSkipShortLivedCertificate(int offsetDays, string expectedResultString)
        {
            // arrange
            var expectedResult = (ChainRevocationCheckResult)Enum.Parse(typeof(ChainRevocationCheckResult), expectedResultString, true);
            var certSubject = "CN=ShortLivedCert CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var rootSubject = "CN=root CN, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var certKeys = CertificateGenerator.GenerateKeysForCertAndItsParent();
            var shortLivedCertificate = CertificateGenerator.GenerateCertificate(certSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(offsetDays), null, certKeys[0]);
            var rootCertificate = CertificateGenerator.GenerateCertificate(rootSubject, rootSubject, DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddDays(300), null, certKeys[1]);
            var chain = CertificateGenerator.CreateChain(new[] { shortLivedCertificate, rootCertificate });
            var config = GetHttpConfig();
            var restRequester = new Mock<IRestRequester>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckChainRevocation(chain);

            // assert
            Assert.AreEqual(expectedResult, result);
        }

        [Test]
        [TestCase("2024-03-14 23:59:59Z", "2024-03-16 00:00:00Z", false)]
        [TestCase("2024-03-15 00:00:00Z", "2024-03-25 00:00:00Z", true)]
        [TestCase("2024-03-15 00:00:00Z", "2024-03-25 00:01:00Z", false)]
        [TestCase("2026-03-15 00:00:00Z", "2026-03-22 00:00:00Z", true)]
        [TestCase("2026-03-15 00:00:00Z", "2026-03-22 00:01:00Z", false)]
        public void TestCheckIfCertificateIsShortLived(string notBeforeString, string notAfterString, bool expectedResult)
        {
            // arrange
            var notBefore = DateTimeOffset.Parse(notBeforeString);
            var notAfter = DateTimeOffset.Parse(notAfterString);
            var certificate = CertificateGenerator.GenerateSelfSignedCertificateWithDefaultSubject("other CA", notBefore, notAfter, null);
            var config = GetHttpConfig();
            var restRequester = new Mock<IRestRequester>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var isShortLived = verifier.IsShortLived(certificate);

            // assert
            Assert.AreEqual(expectedResult, isShortLived);
        }

        [Test]
        [TestCase("CN=other CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland", true)]
        [TestCase("C=Poland, CN=other CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian", true)]
        [TestCase("CN=different CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland", false)]
        public void TestVerifyIfIssuerMatchesTheCertificateIssuer(string issuerName, bool expectedIsEquivalent)
        {
            // arrange
            var certificate = CertificateGenerator.GenerateSelfSignedCertificateWithDefaultSubject("other CA", DateTime.Now.AddYears(-1), DateTime.Now.AddYears(1), new string[][] { });
            var config = GetHttpConfig();
            var restRequester = new Mock<IRestRequester>();
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);
            var crl = new Crl { IssuerName = issuerName };

            // act
            var isEquivalent = verifier.IsIssuerEquivalent(crl, certificate);

            // assert
            Assert.AreEqual(expectedIsEquivalent, isEquivalent);
        }

        private static HttpRequestException NotFoundHttpExceptionProvider() =>
#if NETFRAMEWORK
            new HttpRequestException("Response status code does not indicate success: 404 (Not Found).", null);
#else
            new HttpRequestException("Response status code does not indicate success: 404 (Not Found).", null, HttpStatusCode.NotFound);
#endif

        private static void MockByteResponseForGet(Mock<IRestRequester> restRequester, string url, byte[] bytes)
        {
            restRequester
                .Setup(r => r.Get(
                    It.Is<RestRequestWrapper>(wrapper => wrapper.ToRequestMessage(HttpMethod.Get).RequestUri.AbsoluteUri == url)))
                .Returns(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new ByteArrayContent(bytes)
                });
        }

        private static void MockErrorResponseForGet(Mock<IRestRequester> restRequester, string url, Func<Exception> exceptionProvider)
        {
            restRequester
                .Setup(r => r.Get(
                    It.Is<RestRequestWrapper>(wrapper =>
                        wrapper.ToRequestMessage(HttpMethod.Get).RequestUri.AbsoluteUri == url)))
                .Throws(exceptionProvider);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenCrlExceedsMaxSize()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.LoadFromFile(s_digiCertParentCertificatePath);
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);

            var maxSize = crlBytes.Length - 1; // Set max size to be smaller than actual CRL size
            var config = GetHttpConfig(CertRevocationCheckMode.Enabled, maxSize);

            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenContentLengthHeaderExceedsMaxSize()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var parentCertificate = CertificateGenerator.LoadFromFile(s_digiCertParentCertificatePath);
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var actualCrlSize = crlBytes.Length;

            var maxSize = actualCrlSize - 1; // Set max size to be smaller than actual CRL size
            var config = GetHttpConfig(CertRevocationCheckMode.Enabled, maxSize);

            var restRequester = new Mock<IRestRequester>();
            var mockResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(crlBytes)
            };
            mockResponse.Content.Headers.ContentLength = actualCrlSize;

            restRequester
                .Setup(requester => requester.Get(
                    It.Is<RestRequestWrapper>(wrapper =>
                        wrapper.ToRequestMessage(HttpMethod.Get).RequestUri.AbsoluteUri == DigiCertCrlUrl1)))
                .Returns(mockResponse);

            var crlRepository = new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching);
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls, parentCertificate);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        private HttpClientConfig GetHttpConfig(CertRevocationCheckMode checkMode = CertRevocationCheckMode.Enabled, long crlDownloadMaxSize = 209715200) =>
            new HttpClientConfig(
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                3,
                true,
                checkMode.ToString(),
                false,
                false,
                false,
                10,
                crlDownloadMaxSize);
    }
}
