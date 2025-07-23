using System;
using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
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
            var writer = new AsnWriter(AsnEncodingRules.DER);
            writer.PushSequence(); // Outer SEQUENCE for CRLDistributionPoints
            foreach (var crlUrl in crlUrls)
            {
                writer.PushSequence();
                writer.PushSequence(new Asn1Tag(TagClass.ContextSpecific, 0)); // Tag for distributionPoint
                writer.PushSequence(new Asn1Tag(TagClass.ContextSpecific, 0)); // Tag for fullName (CHOICE)
                writer.PushSequence(); // GeneralNames SEQUENCE
                writer.WriteCharacterString(UniversalTagNumber.IA5String, crlUrl, new Asn1Tag(TagClass.ContextSpecific, 6));
                writer.PopSequence(); // Pop GeneralNames SEQUENCE
                writer.PopSequence(new Asn1Tag(TagClass.ContextSpecific, 0)); // Pop fullName
                writer.PopSequence(new Asn1Tag(TagClass.ContextSpecific, 0)); // Pop distributionPoint
                writer.PopSequence();
            }
            writer.PopSequence(); // Pop outer CRLDistributionPoints SEQUENCE
            return new X509Extension(new Oid("2.5.29.31"), writer.Encode(), false);
        }
    }
}
