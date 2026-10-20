using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Util;

internal sealed class SFSessionHttpClientProxyPropertiesForTests : SFSessionHttpClientProxyProperties
{
    public SFSessionHttpClientProxyPropertiesForTests(
        string proxyHost = null,
        string proxyPort = null,
        string nonProxyHosts = null,
        string proxyPassword = null,
        string proxyUser = null)
        : base(proxyHost, proxyPort, nonProxyHosts, proxyPassword, proxyUser)
    {
    }
}
