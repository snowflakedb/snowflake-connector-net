using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;

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
            var keyPair = GenerateRsaKeyPair(keySize);
            var certGenerator = new X509V3CertificateGenerator();
            var subjectName = $"CN={cn}, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            var distinguishedName = new X509Name(subjectName);
            certGenerator.SetIssuerDN(distinguishedName);
            certGenerator.SetSubjectDN(distinguishedName);
            certGenerator.SetPublicKey(keyPair.Public);
            certGenerator.SetNotBefore(notBefore.UtcDateTime);
            certGenerator.SetNotAfter(notAfter.UtcDateTime);
            certGenerator.SetSerialNumber(BigInteger.ProbablePrime(128, new Random()));
            certGenerator.AddExtension(X509Extensions.BasicConstraints, true, new BasicConstraints(true)); // mark as CA
            if (crlUrls?.Length > 0)
            {
                var distributionPoints = crlUrls
                    .Select(ConvertToDistributionPoint)
                    .ToArray();
                var crlDistPoints = new CrlDistPoint(distributionPoints);
                certGenerator.AddExtension(X509Extensions.CrlDistributionPoints, false, crlDistPoints);
            }
            var signatureFactory = new Asn1SignatureFactory("SHA256WithRSA", keyPair.Private, new SecureRandom());
            var bouncyCertificate = certGenerator.Generate(signatureFactory);
            return ConvertCertificate(bouncyCertificate);
        }

        private static X509Certificate2 ConvertCertificate(Org.BouncyCastle.X509.X509Certificate bouncyCertificate)
        {
            var x509Certificate = DotNetUtilities.ToX509Certificate(bouncyCertificate);
            return new X509Certificate2(x509Certificate);
        }

        private static DistributionPoint ConvertToDistributionPoint(string crlUrl)
        {
            var generalName = new GeneralName(GeneralName.UniformResourceIdentifier, crlUrl);
            var generalNames = new GeneralNames(generalName);
            var distributionPointName = new DistributionPointName(generalNames);
            return new DistributionPoint(distributionPointName, null, null);
        }

        private static AsymmetricCipherKeyPair GenerateRsaKeyPair(int keySize)
        {
            var keyPairGenerator = new RsaKeyPairGenerator();
            keyPairGenerator.Init(new KeyGenerationParameters(new SecureRandom(), keySize));
            return keyPairGenerator.GenerateKeyPair();
        }
    }
}
