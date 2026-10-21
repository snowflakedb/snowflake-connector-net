using System.Web;

namespace Snowflake.Data.Core;

internal class SFSessionHttpClientProxyProperties
{
    internal readonly string proxyHost;
    internal readonly string proxyPort;
    internal readonly string nonProxyHosts;
    internal readonly string proxyPassword;
    internal readonly string proxyUser;

    internal interface IExtractor
    {
        SFSessionHttpClientProxyProperties ExtractProperties(SFSessionProperties propertiesDictionary);
    }

    private protected SFSessionHttpClientProxyProperties(
        string proxyHost,
        string proxyPort,
        string nonProxyHosts,
        string proxyPassword,
        string proxyUser)
    {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.nonProxyHosts = nonProxyHosts;
        this.proxyPassword = proxyPassword;
        this.proxyUser = proxyUser;
    }

    internal class Extractor : IExtractor
    {
        public SFSessionHttpClientProxyProperties ExtractProperties(SFSessionProperties propertiesDictionary)
        {
            if (!bool.Parse(propertiesDictionary[SFSessionProperty.USEPROXY]))
                return new SFSessionHttpClientProxyProperties(null, null, null, null, null);

            // Let's try to get the associated RestRequester
            propertiesDictionary.TryGetValue(SFSessionProperty.PROXYHOST, out var proxyHost);
            propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPORT, out var proxyPort);
            propertiesDictionary.TryGetValue(SFSessionProperty.NONPROXYHOSTS, out var nonProxyHosts);
            propertiesDictionary.TryGetValue(SFSessionProperty.PROXYPASSWORD, out var proxyPassword);
            propertiesDictionary.TryGetValue(SFSessionProperty.PROXYUSER, out var proxyUser);

            if (!string.IsNullOrEmpty(nonProxyHosts))
            {
                // The list is url-encoded
                // Host names are separated with a URL-escaped pipe symbol (%7C).
                nonProxyHosts = HttpUtility.UrlDecode(nonProxyHosts);
            }

            return new SFSessionHttpClientProxyProperties(
                proxyHost: proxyHost,
                proxyPort: proxyPort,
                nonProxyHosts: nonProxyHosts,
                proxyPassword: proxyPassword,
                proxyUser: proxyUser);
        }
    }
}
