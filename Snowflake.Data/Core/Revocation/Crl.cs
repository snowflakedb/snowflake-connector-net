using System;
using System.Collections.Generic;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.X509;

namespace Snowflake.Data.Core.Revocation
{
    internal class Crl
    {
        public DateTime DownloadTime { get; set; }

        public DateTime ThisUpdate { get; set; }

        public DateTime? NextUpdate { get; set; }

        public string IssuerName { get; set; }

        public string[] IssuerDistributionPoints { get; set; }

        public List<string> RevokedCertificates { get; set; }

        public X509Crl BouncyCastleCrl { get; set; }

        public bool IsExpiredOrStale(DateTime now, TimeSpan cacheValidityTime) =>
            NextUpdate < now || DownloadTime.Add(cacheValidityTime) < now;

        public bool IsRevoked(string serialNumber) => RevokedCertificates.Contains(serialNumber);

        public void VerifySignature(AsymmetricKeyParameter publicKey)
        {
            BouncyCastleCrl.Verify(publicKey);
        }

        public byte[] GetEncoded() => BouncyCastleCrl.GetEncoded();
    }
}
