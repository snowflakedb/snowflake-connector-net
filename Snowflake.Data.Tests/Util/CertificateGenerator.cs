using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using Org.BouncyCastle.Asn1.Nist;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;

namespace Snowflake.Data.Tests.Util
{
    public static class CertificateGenerator
    {
        public const string SHA256WithRsaAlgorithm = "SHA256withRSA";
        public const string SHA256WithECDSA = "SHA256withECDSA";

        public static X509Certificate2 LoadFromFile(string filePath) => new(filePath);

        public static X509Certificate2 GenerateSelfSignedCertificateWithDefaultSubject(
            string cn,
            DateTimeOffset notBefore,
            DateTimeOffset notAfter,
            string[][] crlUrls,
            int keySize = 2048)
        {
            var subjectName = $"CN={cn}, O=Snowflake, OU=Drivers, L=Warsaw, ST=Masovian, C=Poland";
            return GenerateSelfSignedCertificate(subjectName, notBefore, notAfter, crlUrls, keySize);
        }

        public static X509Certificate2 GenerateSelfSignedCertificate(
            string subjectName,
            DateTimeOffset notBefore,
            DateTimeOffset notAfter,
            string[][] crlUrls,
            int keySize = 2048)
        {
            var keyPair = GenerateRsaKeyPair(keySize);
            return GenerateCertificate(subjectName, subjectName, notBefore, notAfter, crlUrls, keyPair, true);
        }

        public static X509Certificate2 GenerateCertificate(
            string subjectName,
            string issuerName,
            DateTimeOffset notBefore,
            DateTimeOffset notAfter,
            string[][] crlUrls,
            AsymmetricCipherKeyPair keyPair,
            bool isCA = true,
            string signatureAlgorithm = SHA256WithRsaAlgorithm)
        {
            var certGenerator = new X509V3CertificateGenerator();
            var subjectDistinguishedName = new X509Name(subjectName);
            certGenerator.SetSubjectDN(subjectDistinguishedName);
            var issuerDistinguishedName = new X509Name(issuerName);
            certGenerator.SetIssuerDN(issuerDistinguishedName);
            certGenerator.SetPublicKey(keyPair.Public);
            certGenerator.SetNotBefore(notBefore.UtcDateTime);
            certGenerator.SetNotAfter(notAfter.UtcDateTime);
            certGenerator.SetSerialNumber(BigInteger.ProbablePrime(128, new Random()));
            certGenerator.AddExtension(X509Extensions.BasicConstraints, true, new BasicConstraints(isCA)); // mark as CA
            var keyUsage = isCA ? X509KeyUsageFlags.KeyCertSign : X509KeyUsageFlags.DigitalSignature;
            certGenerator.AddExtension(X509Extensions.KeyUsage, true, new KeyUsage((int)keyUsage));
            if (crlUrls?.Length > 0)
            {
                var distributionPoints = crlUrls
                    .Select(ConvertToDistributionPoint)
                    .ToArray();
                var crlDistPoints = new CrlDistPoint(distributionPoints);
                certGenerator.AddExtension(X509Extensions.CrlDistributionPoints, false, crlDistPoints);
            }
            var signatureFactory = new Asn1SignatureFactory(signatureAlgorithm, keyPair.Private, new SecureRandom());
            var bouncyCertificate = certGenerator.Generate(signatureFactory);
            return ConvertCertificate(bouncyCertificate);
        }

        public static AsymmetricCipherKeyPair[] GenerateKeysForCertAndItsParent(int keySize = 2048)
        {
            var certKeys = GenerateRsaKeyPair(keySize);
            var rootKeys = GenerateRsaKeyPair(keySize);
            return new[] { new AsymmetricCipherKeyPair(certKeys.Public, rootKeys.Private), rootKeys };
        }

        public static AsymmetricCipherKeyPair[] GenerateEllipticKeysForCertAndItsParent()
        {
            var certKeys = GenerateECDSAKeyPair();
            var rootKeys = GenerateECDSAKeyPair();
            return new[] { new AsymmetricCipherKeyPair(certKeys.Public, rootKeys.Private), rootKeys };
        }

        public static X509Chain CreateChain(IEnumerable<X509Certificate2> certificates)
        {
            var certCollection = new X509Certificate2Collection();
            foreach (var certificate in certificates)
            {
                certCollection.Add(certificate);
            }
            var chain = new X509Chain();
            chain.ChainPolicy.ExtraStore.AddRange(certCollection);
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllFlags;
            var isBuilt = chain.Build(certificates.First());
            Assert.IsTrue(isBuilt);
            Assert.AreEqual(certificates.Count(), chain.ChainElements.Count);
            return chain;
        }

        public static X509Crl GenerateCrl(
            string caName,
            DateTime thisUpdateUtc,
            DateTime nextUpdateUtc,
            DateTime revocationTimeUtc)
        {
            var keys = GenerateRsaKeyPair(2048);
            return GenerateCrl(SHA256WithRsaAlgorithm, keys.Private, caName, thisUpdateUtc, nextUpdateUtc, revocationTimeUtc);
        }

        public static X509Crl GenerateCrl(
            string signatureAlgorithm,
            AsymmetricKeyParameter caPrivateKey,
            string caName,
            DateTime thisUpdateUtc,
            DateTime nextUpdateUtc,
            DateTime revocationTimeUtc)
        {
            var crlGenerator = new X509V2CrlGenerator();
            var issuerDN = new X509Name(caName);
            crlGenerator.SetIssuerDN(issuerDN);
            crlGenerator.SetThisUpdate(thisUpdateUtc);
            crlGenerator.SetNextUpdate(nextUpdateUtc);
            crlGenerator.AddCrlEntry(new BigInteger("12345"), revocationTimeUtc, CrlReason.KeyCompromise);
            var signatureFactory = new Asn1SignatureFactory(signatureAlgorithm, caPrivateKey);
            return crlGenerator.Generate(signatureFactory);
        }

        private static X509Certificate2 ConvertCertificate(Org.BouncyCastle.X509.X509Certificate bouncyCertificate)
        {
            var x509Certificate = DotNetUtilities.ToX509Certificate(bouncyCertificate);
            return new X509Certificate2(x509Certificate);
        }

        private static DistributionPoint ConvertToDistributionPoint(string[] crlUrls)
        {
            var generalNameArray = crlUrls.Select(url => new GeneralName(GeneralName.UniformResourceIdentifier, url)).ToArray();
            var generalNames = new GeneralNames(generalNameArray);
            var distributionPointName = new DistributionPointName(generalNames);
            return new DistributionPoint(distributionPointName, null, null);
        }

        private static AsymmetricCipherKeyPair GenerateRsaKeyPair(int keySize)
        {
            var keyPairGenerator = new RsaKeyPairGenerator();
            keyPairGenerator.Init(new KeyGenerationParameters(new SecureRandom(), keySize));
            return keyPairGenerator.GenerateKeyPair();
        }

        public static AsymmetricCipherKeyPair GenerateECDSAKeyPair()
        {
            var ecp = NistNamedCurves.GetByName("P-256");
            var domainParams = new ECDomainParameters(ecp.Curve, ecp.G, ecp.N, ecp.H, ecp.GetSeed());
            var secureRandom = new SecureRandom();
            var keyPairGenerator = new ECKeyPairGenerator();
            var keyGenerationParameters = new ECKeyGenerationParameters(domainParams, secureRandom);
            keyPairGenerator.Init(keyGenerationParameters);
            return keyPairGenerator.GenerateKeyPair();
        }
    }
}
