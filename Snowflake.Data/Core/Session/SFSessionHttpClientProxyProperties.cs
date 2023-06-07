using System;
using System.Web;

namespace Snowflake.Data.Core
{

    internal class SFSessionHttpClientProxyProperties
    {
        internal string proxyHost = null;
        internal string proxyPort = null;
        internal string nonProxyHosts = null;
        internal string proxyPassword = null;
        internal string proxyUser = null;

        internal interface IExtractor
        {
            SFSessionHttpClientProxyProperties ExtractProperties(SFSessionProperties propertiesDictionary);
        }

        internal class Extractor : IExtractor
        {
            public SFSessionHttpClientProxyProperties ExtractProperties(SFSessionProperties propertiesDictionary)
            {
                var properties = new SFSessionHttpClientProxyProperties();
                if (Boolean.Parse(propertiesDictionary[SFSessionProperty.USEPROXY]))
                {
                    // Let's try to get the associated RestRequester
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYHOST, out properties.proxyHost);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPORT, out properties.proxyPort);
                    propertiesDictionary.TryGetValue(SFSessionProperty.NONPROXYHOSTS, out properties.nonProxyHosts);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPASSWORD, out properties.proxyPassword);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYUSER, out properties.proxyUser);

                    if (!String.IsNullOrEmpty(properties.nonProxyHosts))
                    {
                        // The list is url-encoded
                        // Host names are separated with a URL-escaped pipe symbol (%7C). 
                        properties.nonProxyHosts = HttpUtility.UrlDecode(properties.nonProxyHosts);
                    }
                }

                return properties;
            }
        }
    }
}
