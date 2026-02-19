using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    [TestFixture]
    public class SFHttpClientProxyPropertiesTest
    {
        [Test, TestCaseSource(nameof(ProxyPropertiesProvider))]
        public void TestExtractProxyProperties(ProxyPropertiesTestCase testCase)
        {
            // given
            var extractor = new SFSessionHttpClientProxyProperties.Extractor();
            var properties = SFSessionProperties.ParseConnectionString(testCase.conectionString, new SessionPropertiesContext());

            // when
            var proxyProperties = extractor.ExtractProperties(properties);

            // then
            Assert.AreEqual(testCase.expectedProperties.useProxy, proxyProperties.useProxy);
            Assert.AreEqual(testCase.expectedProperties.proxyHost, proxyProperties.proxyHost);
            Assert.AreEqual(testCase.expectedProperties.proxyPort, proxyProperties.proxyPort);
            Assert.AreEqual(testCase.expectedProperties.nonProxyHosts, proxyProperties.nonProxyHosts);
            Assert.AreEqual(testCase.expectedProperties.proxyPassword, proxyProperties.proxyPassword);
            Assert.AreEqual(testCase.expectedProperties.proxyUser, proxyProperties.proxyUser);
        }

        public static IEnumerable<ProxyPropertiesTestCase> ProxyPropertiesProvider()
        {
            var noProxyPropertiesCase = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    useProxy = false,
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
                    useProxy = false,
                    proxyHost = null,
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            var proxyPropertiesConfiguredButNoUseProxyFlag = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;proxyHost=snowflake.com;proxyPort=123;nonProxyHosts=localhost;proxyPassword=proxyPassword;proxyUser=Chris",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    useProxy = false,
                    proxyHost = null,
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            var proxyPropertiesConfiguredAndEnabledCase = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;useProxy=true;proxyHost=snowflake.com;proxyPort=1234",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    useProxy = true,
                    proxyHost = "snowflake.com",
                    proxyPort = "1234",
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
                    useProxy = true,
                    proxyHost = "snowflake.com",
                    proxyPort = "123",
                    nonProxyHosts = "localhost",
                    proxyPassword = "proxyPassword",
                    proxyUser = "Chris"
                }
            };
            var defaultProxyEnabled = new ProxyPropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;useProxy=true;",
                expectedProperties = new SFSessionHttpClientProxyProperties()
                {
                    useProxy = true,
                    proxyHost = null,
                    proxyPort = null,
                    nonProxyHosts = null,
                    proxyPassword = null,
                    proxyUser = null
                }
            };
            return new[]
            {
                noProxyPropertiesCase,
                proxyPropertiesConfiguredButDisabledCase,
                proxyPropertiesConfiguredButNoUseProxyFlag,
                proxyPropertiesConfiguredAndEnabledCase,
                proxyPropertiesAllConfiguredAndEnabled,
                defaultProxyEnabled
            };
        }

        public class ProxyPropertiesTestCase
        {
            internal string conectionString;
            internal SFSessionHttpClientProxyProperties expectedProperties;
        }
    }
}
