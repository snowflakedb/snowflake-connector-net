using System;
using System.Web;

namespace Snowflake.Data.Core
{

    internal class SFSessionHttpClientProxyProperties
    {
        internal bool useProxy = false;
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

                properties.useProxy = Boolean.Parse(propertiesDictionary[SFSessionProperty.USEPROXY]);

                if (properties.useProxy)
                {
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYHOST, out properties.proxyHost);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPORT, out properties.proxyPort);
                    propertiesDictionary.TryGetValue(SFSessionProperty.NONPROXYHOSTS, out properties.nonProxyHosts);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPASSWORD, out properties.proxyPassword);
                    propertiesDictionary.TryGetValue(SFSessionProperty.PROXYUSER, out properties.proxyUser);

                    if (!String.IsNullOrEmpty(properties.nonProxyHosts))
                    {
                        properties.nonProxyHosts = HttpUtility.UrlDecode(properties.nonProxyHosts);
                    }
                }

                return properties;
            }
        }
    }
}
