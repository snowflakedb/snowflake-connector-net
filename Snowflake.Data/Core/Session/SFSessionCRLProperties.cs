using System;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Core.Session;

internal readonly record struct SFSessionCRLProperties(
    CertRevocationCheckMode CertRevocationCheckMode,
    bool EnableCrlDiskCaching,
    bool EnableCrlInMemoryCaching,
    bool AllowCertificatesWithoutCrlUrl,
    int CRLDownloadTimeout,
    long CRLDownloadMaxSize)
{
    internal interface IExtractor
    {
        SFSessionCRLProperties ExtractProperties(SFSessionProperties propertiesDictionary);
    }

    internal sealed class Extractor : IExtractor
    {
        public SFSessionCRLProperties ExtractProperties(SFSessionProperties propertiesDictionary)
        {
            return new SFSessionCRLProperties(
                CertRevocationCheckMode: (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode),
                    propertiesDictionary[SFSessionProperty.CERTREVOCATIONCHECKMODE], true),
                EnableCrlDiskCaching: bool.Parse(propertiesDictionary[SFSessionProperty.ENABLECRLDISKCACHING]),
                EnableCrlInMemoryCaching: bool.Parse(propertiesDictionary[SFSessionProperty.ENABLECRLINMEMORYCACHING]),
                AllowCertificatesWithoutCrlUrl: bool.Parse(propertiesDictionary[SFSessionProperty.ALLOWCERTIFICATESWITHOUTCRLURL]),
                CRLDownloadTimeout: int.Parse(propertiesDictionary[SFSessionProperty.CRLDOWNLOADTIMEOUT]),
                CRLDownloadMaxSize: long.Parse(propertiesDictionary[SFSessionProperty.CRLDOWNLOADMAXSIZE])
            );
        }
    }
}
