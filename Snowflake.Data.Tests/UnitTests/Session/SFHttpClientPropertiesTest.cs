using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SFHttpClientPropertiesTest
    {
        [Test]
        public void TestConvertToMapOnly2Properties(
            [Values(true, false)] bool validateDefaultParameters,
            [Values(true, false)] bool clientSessionKeepAlive,
            [Values(true, false)] bool clientStoreTemporaryCredential)
        {
            // arrange
            var proxyProperties = new SFSessionHttpClientProxyProperties()
            {
                proxyHost = "localhost",
                proxyPort = "1234",
                nonProxyHosts = "snowflakecomputing.com",
                proxyPassword = "test",
                proxyUser = "test"
            };
            var properties = new SFSessionHttpClientProperties()
            {
                validateDefaultParameters = validateDefaultParameters,
                clientSessionKeepAlive = clientSessionKeepAlive,
                _clientStoreTemporaryCredential = clientStoreTemporaryCredential,
                connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                insecureMode = false,
                disableRetry = false,
                forceRetryOn404 = false,
                retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                maxHttpRetries = 7,
                proxyProperties = proxyProperties
            };

            // act
            var parameterMap = properties.ToParameterMap();

            // assert
            Assert.AreEqual(3, parameterMap.Count);
            Assert.AreEqual(validateDefaultParameters, parameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS]);
            Assert.AreEqual(clientSessionKeepAlive, parameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE]);
            Assert.AreEqual(clientStoreTemporaryCredential, parameterMap[SFSessionParameter.CLIENT_STORE_TEMPORARY_CREDENTIAL]);
        }

        [Test]
        public void TestBuildHttpClientConfig()
        {
            // arrange
            var properties = RandomSFSessionHttpClientProperties();

            // act
            var config = properties.BuildHttpClientConfig();

            // assert
            Assert.AreEqual(!properties.insecureMode, config.CrlCheckEnabled);
            Assert.AreEqual(properties.proxyProperties.proxyHost, config.ProxyHost);
            Assert.AreEqual(properties.proxyProperties.proxyPort, config.ProxyPort);
            Assert.AreEqual(properties.proxyProperties.proxyUser, config.ProxyUser);
            Assert.AreEqual(properties.proxyProperties.proxyPassword, config.ProxyPassword);
            Assert.AreEqual(properties.proxyProperties.nonProxyHosts, config.NoProxyList);
            Assert.AreEqual(properties.disableRetry, config.DisableRetry);
            Assert.AreEqual(properties.forceRetryOn404, config.ForceRetryOn404);
            Assert.AreEqual(properties.maxHttpRetries, config.MaxHttpRetries);
        }

        [Test]
        public void TestCrlCheckEnabledToBeOppositeInsecureMode([Values] bool insecureMode)
        {
            // arrange
            var properties = RandomSFSessionHttpClientProperties();
            properties.insecureMode = insecureMode;

            // act
            var config = properties.BuildHttpClientConfig();

            // assert
            Assert.AreEqual(!insecureMode, config.CrlCheckEnabled);
        }

        private SFSessionHttpClientProperties RandomSFSessionHttpClientProperties()
        {
            var proxyProperties = new SFSessionHttpClientProxyProperties()
            {
                proxyHost = TestDataGenarator.NextAlphaNumeric(),
                proxyPort = TestDataGenarator.NextDigitsString(4),
                nonProxyHosts = TestDataGenarator.NextAlphaNumeric(),
                proxyPassword = TestDataGenarator.NextAlphaNumeric(),
                proxyUser = TestDataGenarator.NextAlphaNumeric()
            };
            return new SFSessionHttpClientProperties()
            {
                validateDefaultParameters = TestDataGenarator.NextBool(),
                clientSessionKeepAlive = TestDataGenarator.NextBool(),
                connectionTimeout = TimeSpan.FromSeconds(TestDataGenarator.NextInt(30, 151)),
                insecureMode = TestDataGenarator.NextBool(),
                disableRetry = TestDataGenarator.NextBool(),
                forceRetryOn404 = TestDataGenarator.NextBool(),
                retryTimeout = TimeSpan.FromSeconds(TestDataGenarator.NextInt(300, 600)),
                maxHttpRetries = TestDataGenarator.NextInt(0, 15),
                proxyProperties = proxyProperties
            };
        }

        [Test, TestCaseSource(nameof(PropertiesProvider))]
        public void TestExtractProperties(PropertiesTestCase testCase)
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString(testCase.conectionString, new SessionPropertiesContext());
            var proxyProperties = new SFSessionHttpClientProxyProperties();

            // act
            var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

            // assert
            Assert.AreEqual(testCase.expectedProperties.validateDefaultParameters, extractedProperties.validateDefaultParameters);
            Assert.AreEqual(testCase.expectedProperties.clientSessionKeepAlive, extractedProperties.clientSessionKeepAlive);
            Assert.AreEqual(testCase.expectedProperties.connectionTimeout, extractedProperties.connectionTimeout);
            Assert.AreEqual(testCase.expectedProperties.insecureMode, extractedProperties.insecureMode);
            Assert.AreEqual(testCase.expectedProperties.disableRetry, extractedProperties.disableRetry);
            Assert.AreEqual(testCase.expectedProperties.forceRetryOn404, extractedProperties.forceRetryOn404);
            Assert.AreEqual(testCase.expectedProperties.retryTimeout, extractedProperties.retryTimeout);
            Assert.AreEqual(testCase.expectedProperties.maxHttpRetries, extractedProperties.maxHttpRetries);
            Assert.NotNull(proxyProperties);
        }

        public static IEnumerable<PropertiesTestCase> PropertiesProvider()
        {
            var defaultProperties = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithValidateDefaultParametersChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;validate_default_parameters=false",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = false,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithClientSessionKeepAliveChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;client_session_keep_alive=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = true,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithTimeoutChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;connection_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = TimeSpan.FromSeconds(15),
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithInsecureModeChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;insecureMode=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = true,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithDisableRetryChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;disableRetry=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = true,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithForceRetryOn404Changed = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;forceRetryOn404=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = true,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueAbove300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=600",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = TimeSpan.FromSeconds(600),
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueBelow300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithRetryTimeoutChangedToZero = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultConnectionTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = TimeoutHelper.Infinity(),
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithMaxHttpRetriesChangedToAValueAbove7 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=10",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = 10
                }
            };
            var propertiesWithMaxHttpRetriesChangedToAValueBelow7 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=5",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries
                }
            };
            var propertiesWithMaxHttpRetriesChangedToZero = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = 0
                }
            };
            return new[]
            {
                defaultProperties,
                propertiesWithValidateDefaultParametersChanged,
                propertiesWithClientSessionKeepAliveChanged,
                propertiesWithTimeoutChanged,
                propertiesWithInsecureModeChanged,
                propertiesWithDisableRetryChanged,
                propertiesWithForceRetryOn404Changed,
                propertiesWithRetryTimeoutChangedToAValueAbove300,
                propertiesWithRetryTimeoutChangedToAValueBelow300,
                propertiesWithRetryTimeoutChangedToZero,
                propertiesWithMaxHttpRetriesChangedToAValueAbove7,
                propertiesWithMaxHttpRetriesChangedToAValueBelow7,
                propertiesWithMaxHttpRetriesChangedToZero
            };
        }

        public class PropertiesTestCase
        {
            internal string conectionString;
            internal SFSessionHttpClientProperties expectedProperties;
        }
    }
}
