using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;

namespace Snowflake.Data.Core.Revocation
{
    internal class CertificateCrlDistributionPointsExtractor
    {
        private const string CrlDistributionPointsOid = "2.5.29.31";

        public static CertificateCrlDistributionPointsExtractor Instance = new CertificateCrlDistributionPointsExtractor();

        internal CertificateCrlDistributionPointsExtractor()
        {
        }

        public string[] Extract(X509Certificate2 certificate)
        {
            var extensions = certificate.Extensions;
            if (extensions == null)
                return Array.Empty<string>();
            foreach (var extension in extensions)
            {
                if (extension.Oid?.Value == CrlDistributionPointsOid)
                {
                    var asn1Object = ReadAsn1Object(extension.RawData);
                    var crlDistPoint = CrlDistPoint.GetInstance(asn1Object);
                    return ReadCrlDistributionPoints(crlDistPoint);
                }
            }
            return Array.Empty<string>();
        }

        private Asn1Object ReadAsn1Object(byte[] rawData)
        {
            using (var asn1InputStream = new Asn1InputStream(rawData))
            {
                return asn1InputStream.ReadObject();
            }
        }

        private string[] ReadCrlDistributionPoints(CrlDistPoint crlDistPoint) =>
            crlDistPoint.GetDistributionPoints()
                .Select(distributionPoint => distributionPoint.DistributionPointName)
                .Where(distributionPointName => distributionPointName != null)
                .Select(distributionPointName => GeneralNames.GetInstance(distributionPointName.Name))
                .SelectMany(names => names.GetNames())
                .Where(name => name.TagNo == GeneralName.UniformResourceIdentifier)
                .Select(name => ((DerStringBase)name.Name).GetString())
                .Where(url => url.StartsWith("http://", StringComparison.InvariantCultureIgnoreCase))
                .Distinct()
                .ToArray();
    }
}
