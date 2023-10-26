/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections.Generic;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    [TestFixture]
    public class SFHttpClientPropertiesTest
    {
        [Test]
        public void ShouldConvertToMapOnly2Properties(
            [Values(true, false)] bool validateDefaultParameters,
            [Values(true, false)] bool clientSessionKeepAlive)
        {
            // given
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
                timeoutInSec = BaseRestRequest.DEFAULT_REST_RETRY_SECONDS_TIMEOUT,
                insecureMode = false,
                disableRetry = false,
                forceRetryOn404 = false,
                maxHttpRetries = 7,
                proxyProperties = proxyProperties
            };
            
            // when
            var parameterMap = properties.ToParameterMap();
            
            // then
            Assert.AreEqual(2, parameterMap.Count);
            Assert.AreEqual(validateDefaultParameters, parameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS]);
            Assert.AreEqual(clientSessionKeepAlive, parameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE]);
        }

        [Test]
        public void ShouldBuildHttpClientConfig()
        {
            // given
            var proxyProperties = new SFSessionHttpClientProxyProperties()
            {
                proxyHost = TestDataGenarator.NextAlphaNumeric(),
                proxyPort = TestDataGenarator.NextDigitsString(4),
                nonProxyHosts = TestDataGenarator.NextAlphaNumeric(),
                proxyPassword = TestDataGenarator.NextAlphaNumeric(),
                proxyUser = TestDataGenarator.NextAlphaNumeric()
            };
            var properties = new SFSessionHttpClientProperties()
            {
                validateDefaultParameters = TestDataGenarator.NextBool(),
                clientSessionKeepAlive = TestDataGenarator.NextBool(),
                timeoutInSec = TestDataGenarator.NextInt(30, 151),
                insecureMode = TestDataGenarator.NextBool(),
                disableRetry = TestDataGenarator.NextBool(),
                forceRetryOn404 = TestDataGenarator.NextBool(),
                maxHttpRetries = TestDataGenarator.NextInt(0, 15),
                proxyProperties = proxyProperties
            };
            
            // when
            var config = properties.BuildHttpClientConfig();

            // then
            Assert.AreEqual(properties.insecureMode, config.CrlCheckEnabled);
            Assert.AreEqual(properties.proxyProperties.proxyHost, config.ProxyHost);
            Assert.AreEqual(properties.proxyProperties.proxyPort, config.ProxyPort);
            Assert.AreEqual(properties.proxyProperties.proxyUser, config.ProxyUser);
            Assert.AreEqual(properties.proxyProperties.proxyPassword, config.ProxyPassword);
            Assert.AreEqual(properties.proxyProperties.nonProxyHosts, config.NoProxyList);
            Assert.AreEqual(properties.disableRetry, config.DisableRetry);
            Assert.AreEqual(properties.forceRetryOn404, config.ForceRetryOn404);
            Assert.AreEqual(properties.maxHttpRetries, config.MaxHttpRetries);
        }

        [Test, TestCaseSource(nameof(PropertiesProvider))]
        public void ShouldExtractProperties(PropertiesTestCase testCase)
        {
            // given
            var proxyExtractorMock = new Moq.Mock<SFSessionHttpClientProxyProperties.IExtractor>();
            var extractor = new SFSessionHttpClientProperties.Extractor(proxyExtractorMock.Object);
            var properties = SFSessionProperties.parseConnectionString(testCase.conectionString, null);
            var proxyProperties = new SFSessionHttpClientProxyProperties();
            proxyExtractorMock
                .Setup(e => e.ExtractProperties(properties))
                .Returns(proxyProperties);

            // when
            var extractedProperties = extractor.ExtractProperties(properties);

            // then
            Assert.AreEqual(testCase.expectedProperties.validateDefaultParameters, extractedProperties.validateDefaultParameters);
            Assert.AreEqual(testCase.expectedProperties.clientSessionKeepAlive, extractedProperties.clientSessionKeepAlive);
            Assert.AreEqual(testCase.expectedProperties.timeoutInSec, extractedProperties.timeoutInSec);
            Assert.AreEqual(testCase.expectedProperties.insecureMode, extractedProperties.insecureMode);
            Assert.AreEqual(testCase.expectedProperties.disableRetry, extractedProperties.disableRetry);
            Assert.AreEqual(testCase.expectedProperties.forceRetryOn404, extractedProperties.forceRetryOn404);
            Assert.AreEqual(testCase.expectedProperties.maxHttpRetries, extractedProperties.maxHttpRetries);
            Assert.AreEqual(proxyProperties, extractedProperties.proxyProperties);
            proxyExtractorMock.Verify(e => e.ExtractProperties(properties), Times.Once);
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
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithValidateDefaultParametersChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;validate_default_parameters=false",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = false,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithClientSessionKeepAliveChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;client_session_keep_alive=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = true,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithTimeoutChangedToAValueAbove300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;connection_timeout=600",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = 600,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithTimeoutChangedToAValueBelow300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;connection_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = 300,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithInsecureModeChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;insecureMode=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = true,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithDisableRetryChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;disableRetry=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = true,
                    forceRetryOn404 = false,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithForceRetryOn404Changed = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;forceRetryOn404=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = true,
                    maxHttpRetries = 7
                }
            };
            var propertiesWithMaxHttpRetiesChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=10",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionProperties.s_connectionTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    maxHttpRetries = 10
                }
            };
            return new []
            {
                defaultProperties,
                propertiesWithValidateDefaultParametersChanged,
                propertiesWithClientSessionKeepAliveChanged,
                propertiesWithTimeoutChangedToAValueAbove300,
                propertiesWithTimeoutChangedToAValueBelow300,
                propertiesWithInsecureModeChanged,
                propertiesWithDisableRetryChanged,
                propertiesWithForceRetryOn404Changed,
                propertiesWithMaxHttpRetiesChanged
            };
        }

        public class PropertiesTestCase
        {
            internal string conectionString;
            internal SFSessionHttpClientProperties expectedProperties;
        }
    }
}
