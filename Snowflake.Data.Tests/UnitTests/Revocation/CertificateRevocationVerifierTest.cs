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

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CertificateRevocationVerifierTest : RevocationTests
    {
        [Test]
        public void TestVerifyCertificateAsUnrevoked()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockByteResponseForGet(restRequester, DigiCertCrlUrl2, crlBytes);
            var crlRepository = new Mock<CrlRepository>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, Core.Tools.TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository.Object);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertUnrevoked, result);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenCouldNotDownloadCrl()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockErrorResponseForGet(restRequester, DigiCertCrlUrl2, () => new HttpRequestException("Response status code does not indicate success: 404 (Not Found).", null, HttpStatusCode.NotFound));
            var crlRepository = new Mock<CrlRepository>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, Core.Tools.TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository.Object);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        public void TestVerifyCertificateAsErrorWhenOneOfCrlsIsNotParsable()
        {
            // arrange
            var expectedCrlUrls = new[] { DigiCertCrlUrl1, DigiCertCrlUrl2 };
            var certificate = CertificateGenerator.LoadFromFile(s_digiCertCertificatePath);
            var config = GetHttpConfig();
            var crlBytes = File.ReadAllBytes(s_digiCertCrlPath);
            var notParsableCrlBytes = Encoding.ASCII.GetBytes("not parsable crl");
            var restRequester = new Mock<IRestRequester>();
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, crlBytes);
            MockByteResponseForGet(restRequester, DigiCertCrlUrl1, notParsableCrlBytes);
            var crlRepository = new Mock<CrlRepository>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, Core.Tools.TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository.Object);

            // act
            var result = verifier.CheckCertRevocation(certificate, expectedCrlUrls);

            // assert
            Assert.AreEqual(CertRevocationCheckResult.CertError, result);
        }

        [Test]
        [TestCase("CN=other CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland", true)]
        [TestCase("C=Poland, CN=other CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian", true)]
        [TestCase("CN=different CA, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland", false)]
        public void TestVerifyIfIssuerMatchesTheCertificateIssuer(string issuerName, bool expectedIsEquivalent)
        {
            // arrange
            var certificate = CertificateGenerator.GenerateSelfSignedCertificate("other CA", DateTime.Now.AddYears(-1), DateTime.Now.AddYears(1), new string[] { });
            var config = GetHttpConfig();
            var restRequester = new Mock<IRestRequester>();
            var crlRepository = new Mock<CrlRepository>();
            var environmentOperation = new Mock<EnvironmentOperations>();
            var verifier = new CertificateRevocationVerifier(config, Core.Tools.TimeProvider.Instance, restRequester.Object, CertificateCrlDistributionPointsExtractor.Instance, new CrlParser(environmentOperation.Object), crlRepository.Object);
            var crl = new Crl { IssuerName = issuerName };

            // act
            var isEquivalent = verifier.IsIssuerEquivalent(crl, certificate);

            // assert
            Assert.AreEqual(expectedIsEquivalent, isEquivalent);
        }

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

        private HttpClientConfig GetHttpConfig() =>
            new HttpClientConfig(
                true,
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                3,
                true,
                false,
                CertRevocationCheckMode.Enabled.ToString(),
                true,
                true,
                false);
    }
}
