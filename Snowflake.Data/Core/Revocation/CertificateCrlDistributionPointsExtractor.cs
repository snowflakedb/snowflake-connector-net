using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;

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
                    return ReadCrlDistributionPoints(extension);
            }
            return Array.Empty<string>();
        }

        private string[] ReadCrlDistributionPoints(X509Extension extension)
        {
            var rawData = extension.RawData;
            List<string> crlUrls = new List<string>();

            // Search for URI tags (0x86) and extract the subsequent string
            int i = 0;
            while (i < rawData.Length)
            {
                // Look for the tag that signifies a GeneralName (Context-specific, Tag 6 for URI)
                // This is a common pattern for URIs in this extension
                if (rawData[i] == 0x86) // ASN.1 tag for Context-specific, Primitive, Tag 6 (GeneralName: UniformResourceIdentifier)
                {
                    // The next byte is the length of the URI string
                    if (i + 1 < rawData.Length)
                    {
                        int length = rawData[i + 1];
                        if (i + 2 + length <= rawData.Length)
                        {
                            string url = Encoding.ASCII.GetString(rawData, i + 2, length);
                            crlUrls.Add(url);
                            i += (2 + length); // Move past this URI
                        }
                        else
                        {
                            // Malformed length
                            break;
                        }
                    }
                    else
                    {
                        // Malformed tag-length-value
                        break;
                    }
                }
                else
                {
                    // Move to the next byte, hoping to find a tag, or implement more robust ASN.1 TLV parsing
                    i++;
                }
            }
            return crlUrls.ToArray();
        }
    }
}
