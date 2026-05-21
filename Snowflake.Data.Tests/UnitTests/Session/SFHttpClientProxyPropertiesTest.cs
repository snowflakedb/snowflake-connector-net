using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    [TestFixture]
    public class SFHttpClientProxyPropertiesTest
    {
        [Test, TestCaseSource(nameof(ProxyPropertiesProvider))]
        public void ShouldExtractProxyProperties(ProxyPropertiesTestCase testCase)
        {
            // given
            var extractor = new SFSessionHttpClientProxyProperties.Extractor();
            var properties = SFSessionProperties.ParseConnectionString(testCase.conectionString, new SessionPropertiesContext());

            // when
            var proxyProperties = extractor.ExtractProperties(properties);

            // then
            Assert.Equal(testCase.expectedProperties.proxyHost, proxyProperties.proxyHost);
            Assert.Equal(testCase.expectedProperties.proxyPort, proxyProperties.proxyPort);
            Assert.Equal(testCase.expectedProperties.nonProxyHosts, proxyProperties.nonProxyHosts);
            Assert.Equal(testCase.expectedProperties.proxyPassword, proxyProperties.proxyPassword);
            Assert.Equal(testCase.expectedProperties.proxyUser, proxyProperties.proxyUser);
        }

        public static IEnumerable<ProxyPropertiesTestCase> ProxyPropertiesProvider()
        {
            var noProxyPropertiesCase = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    proxyHost = null,
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            var proxyPropertiesConfiguredButDisabledCase = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;useProxy=false;proxyHost=snowflake.com;proxyPort=123;nonProxyHosts=localhost;proxyPassword=proxyPassword;proxyUser=Chris",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    proxyHost = null,
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            var proxyPropertiesConfiguredAndEnabledCase = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;useProxy=true;proxyHost=snowflake.com",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    proxyHost = "snowflake.com",
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            var proxyPropertiesAllConfiguredAndEnabled = new ProxyPropertiesTestCase()
            {
                conectionString =
                    "account=test;user=test;password=test;useProxy=true;proxyHost=snowflake.com;proxyPort=123;nonProxyHosts=localhost;proxyPassword=proxyPassword;proxyUser=Chris",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    proxyHost = "snowflake.com",
                    proxyPort = "123",
                    nonProxyHosts = "localhost",
                    proxyPassword = "proxyPassword",
                    proxyUser = "Chris"
                }
            };
            return new[]
            {
                noProxyPropertiesCase,
                proxyPropertiesConfiguredButDisabledCase,
                proxyPropertiesConfiguredAndEnabledCase,
                proxyPropertiesAllConfiguredAndEnabled
            };
        }

        public class ProxyPropertiesTestCase
        {
            internal string conectionString;
            internal SFSessionHttpClientProxyProperties expectedProperties;
        }
    }
}
