using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public class SFHttpClientProxyPropertiesTest
{
    [SFTheory, MemberData(nameof(ProxyPropertiesProvider))]
    public void ShouldExtractProxyProperties(ProxyPropertiesTestCase testCase)
    {
        // given
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();
        var properties = SFSessionProperties.ParseConnectionString(testCase.conectionString, new SessionPropertiesContext());

        // when
        var proxyProperties = extractor.ExtractProperties(properties);

        // then
        Assert.Equal(testCase.expectedProxyHost, proxyProperties.proxyHost);
        Assert.Equal(testCase.expectedProxyPort, proxyProperties.proxyPort);
        Assert.Equal(testCase.expectedNonProxyHosts, proxyProperties.nonProxyHosts);
        Assert.Equal(testCase.expectedProxyPassword, proxyProperties.proxyPassword);
        Assert.Equal(testCase.expectedProxyUser, proxyProperties.proxyUser);
    }

    public static IEnumerable<object[]> ProxyPropertiesProvider()
    {
        var noProxyPropertiesCase = new ProxyPropertiesTestCase
        {
            conectionString = "account=test;user=test;password=test"
        };
        var proxyPropertiesConfiguredButDisabledCase = new ProxyPropertiesTestCase
        {
            conectionString = "account=test;user=test;password=test;useProxy=false;proxyHost=snowflake.com;proxyPort=123;nonProxyHosts=localhost;proxyPassword=proxyPassword;proxyUser=Chris"
        };
        var proxyPropertiesConfiguredAndEnabledCase = new ProxyPropertiesTestCase
        {
            conectionString = "account=test;user=test;password=test;useProxy=true;proxyHost=snowflake.com",
            expectedProxyHost = "snowflake.com"
        };
        var proxyPropertiesAllConfiguredAndEnabled = new ProxyPropertiesTestCase
        {
            conectionString =
                "account=test;user=test;password=test;useProxy=true;proxyHost=snowflake.com;proxyPort=123;nonProxyHosts=localhost;proxyPassword=proxyPassword;proxyUser=Chris",
            expectedProxyHost = "snowflake.com",
            expectedProxyPort = "123",
            expectedNonProxyHosts = "localhost",
            expectedProxyPassword = "proxyPassword",
            expectedProxyUser = "Chris"
        };
        return new object[][]
        {
            new object[] { noProxyPropertiesCase },
            new object[] { proxyPropertiesConfiguredButDisabledCase },
            new object[] { proxyPropertiesConfiguredAndEnabledCase },
            new object[] { proxyPropertiesAllConfiguredAndEnabled }
        };
    }

    public class ProxyPropertiesTestCase
    {
        internal string conectionString;
        internal string expectedProxyHost;
        internal string expectedProxyPort;
        internal string expectedNonProxyHosts;
        internal string expectedProxyPassword;
        internal string expectedProxyUser;
    }
}
