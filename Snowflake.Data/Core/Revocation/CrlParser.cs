using System;
using System.Linq;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.X509;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Revocation
{
    internal class CrlParser
    {
        internal const string CrlValidityTimeEnvName = "SF_CRL_VALIDITY_TIME";
        private const int CrlValidityTimeDefaultDays = 10;

        private readonly TimeSpan _crlCacheValidityTime;

        public CrlParser(EnvironmentOperations environmentOperations)
        {
            var validityDays = ValuesExtractor.ExtractInt(
                () => environmentOperations.GetEnvironmentVariable(CrlValidityTimeEnvName),
                $"environmental variable {CrlValidityTimeEnvName}",
                CrlValidityTimeDefaultDays);
            _crlCacheValidityTime = TimeSpan.FromDays(validityDays);
        }

        internal CrlParser(TimeSpan crlCacheValidityTime)
        {
            _crlCacheValidityTime = crlCacheValidityTime;
        }

        public Crl Parse(byte[] bytes, DateTime now)
        {
            var crlParser = new X509CrlParser();
            var parsedCrl = crlParser.ReadCrl(bytes);
            if (parsedCrl == null)
                return null;
            var crl = new Crl
            {
                DownloadTime = now,
                ThisUpdate = parsedCrl.ThisUpdate,
                NextUpdate = parsedCrl.NextUpdate,
                IssuerName = parsedCrl.IssuerDN.ToString(),
                IssuerDistributionPoints = ReadIdpFromCrl(parsedCrl),
                RevokedCertificates = parsedCrl.GetRevokedCertificates().Select(cert => ConvertToHexadecimalString(cert.SerialNumber)).ToList(),
                CrlCacheValidityTime = _crlCacheValidityTime
            };
            return crl;
        }

        private string[] ReadIdpFromCrl(X509Crl crl)
        {
            var idpExtension = crl.GetExtensionValue(X509Extensions.IssuingDistributionPoint);
            if (idpExtension == null)
                return Array.Empty<string>();
            var idpAsAsnObject = Asn1Object.FromByteArray(idpExtension.GetOctets());
            var idp = IssuingDistributionPoint.GetInstance(idpAsAsnObject);
            var distributionPoints = (GeneralNames)idp.DistributionPoint?.Name;
            if (distributionPoints == null)
                return Array.Empty<string>();
            var names = distributionPoints.GetNames().Select(n => n.Name.ToString()).ToArray();
            return names;
        }

        internal string ConvertToHexadecimalString(BigInteger value)
        {
            var bytes = value.ToByteArray();
            var hexString = BitConverter.ToString(bytes)
                .Replace("-", string.Empty)
                .ToUpper();
            return hexString;
        }
    }
}
