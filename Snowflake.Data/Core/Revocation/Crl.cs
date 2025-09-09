using System;
using System.Collections.Generic;

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

        public TimeSpan CrlCacheValidityTime { get; set; } = TimeSpan.FromDays(10);

        public bool NeedsFreshCrl(DateTime now) =>
            NextUpdate < now || DownloadTime.Add(CrlCacheValidityTime) < now;

        public bool IsRevoked(string serialNumber) => RevokedCertificates.Contains(serialNumber);
    }
}
