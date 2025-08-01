using System;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using X509Extension = System.Security.Cryptography.X509Certificates.X509Extension;

namespace Snowflake.Data.Tests.Util
{
    public static class CertificateGenerator
    {
        public static X509Certificate2 LoadFromFile(string filePath) => new(filePath);

        public static X509Certificate2 GenerateSelfSignedCertificate(
            string cn,
            DateTimeOffset notBefore,
            DateTimeOffset notAfter,
            string[] crlUrls,
            int keySize = 2048)
        {
            var subjectName = $"CN={cn}, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var distinguishedName = new X500DistinguishedName(subjectName);
            using (var rsa = RSA.Create(keySize))
            {
                var request = new CertificateRequest(
                    distinguishedName,
                    rsa,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1);
                request.CertificateExtensions.Add(
                    new X509BasicConstraintsExtension(
                        certificateAuthority: false,
                        hasPathLengthConstraint: false,
                        pathLengthConstraint: 0,
                        critical: true
                    ));
                request.CertificateExtensions.Add(
                    new X509KeyUsageExtension(
                        X509KeyUsageFlags.DataEncipherment |
                        X509KeyUsageFlags.KeyEncipherment |
                        X509KeyUsageFlags.DigitalSignature,
                        critical: true));
                var sanBuilder = new SubjectAlternativeNameBuilder();
                sanBuilder.AddDnsName("localhost");
                sanBuilder.AddDnsName("127.0.0.1");
                request.CertificateExtensions.Add(sanBuilder.Build());
                request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
                if (crlUrls.Length > 0)
                {
                    var distributionPointsExtension = CreateCrlDistributionPointsExtension(crlUrls);
                    request.CertificateExtensions.Add(distributionPointsExtension);
                }
                var certificate = request.CreateSelfSigned(notBefore, notAfter);
                return certificate;
            }
        }

        private static X509Extension CreateCrlDistributionPointsExtension(string[] crlUrls)
        {
            var distributionPoints = crlUrls
                .Select(crlUrl =>
                {
                    var uriGeneralName = new GeneralName(GeneralName.UniformResourceIdentifier, crlUrl);
                    var dpName = new DistributionPointName(DistributionPointName.FullName, new GeneralNames(uriGeneralName));
                    return new DistributionPoint(dpName, null, null);
                })
                .ToArray();
            var crlDistPoint = new CrlDistPoint(distributionPoints);
            return new X509Extension(new Oid("2.5.29.31"), crlDistPoint.GetDerEncoded(), false);
        }
    }
}
