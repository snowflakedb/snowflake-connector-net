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
        public void TestConvertToMapOnly2Properties(
            [Values(true, false)] bool validateDefaultParameters,
            [Values(true, false)] bool clientSessionKeepAlive)
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
                timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                insecureMode = false,
                disableRetry = false,
                forceRetryOn404 = false,
                retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                maxHttpRetries = 7,
                proxyProperties = proxyProperties
            };
            
            // act
            var parameterMap = properties.ToParameterMap();
            
            // assert
            Assert.AreEqual(2, parameterMap.Count);
            Assert.AreEqual(validateDefaultParameters, parameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS]);
            Assert.AreEqual(clientSessionKeepAlive, parameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE]);
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
                timeoutInSec = TestDataGenarator.NextInt(30, 151),
                insecureMode = TestDataGenarator.NextBool(),
                disableRetry = TestDataGenarator.NextBool(),
                forceRetryOn404 = TestDataGenarator.NextBool(),
                retryTimeout = TestDataGenarator.NextInt(300, 600),
                maxHttpRetries = TestDataGenarator.NextInt(0, 15),
                proxyProperties = proxyProperties
            };
        }

        [Test, TestCaseSource(nameof(PropertiesProvider))]
        public void TestExtractProperties(PropertiesTestCase testCase)
        {
            // arrange
            var proxyExtractorMock = new Moq.Mock<SFSessionHttpClientProxyProperties.IExtractor>();
            var extractor = new SFSessionHttpClientProperties.Extractor(proxyExtractorMock.Object);
            var properties = SFSessionProperties.parseConnectionString(testCase.conectionString, null);
            var proxyProperties = new SFSessionHttpClientProxyProperties();
            proxyExtractorMock
                .Setup(e => e.ExtractProperties(properties))
                .Returns(proxyProperties);

            // act
            var extractedProperties = extractor.ExtractProperties(properties);
            extractedProperties.CheckPropertiesAreValid();

            // assert
            Assert.AreEqual(testCase.expectedProperties.validateDefaultParameters, extractedProperties.validateDefaultParameters);
            Assert.AreEqual(testCase.expectedProperties.clientSessionKeepAlive, extractedProperties.clientSessionKeepAlive);
            Assert.AreEqual(testCase.expectedProperties.timeoutInSec, extractedProperties.timeoutInSec);
            Assert.AreEqual(testCase.expectedProperties.insecureMode, extractedProperties.insecureMode);
            Assert.AreEqual(testCase.expectedProperties.disableRetry, extractedProperties.disableRetry);
            Assert.AreEqual(testCase.expectedProperties.forceRetryOn404, extractedProperties.forceRetryOn404);
            Assert.AreEqual(testCase.expectedProperties.retryTimeout, extractedProperties.retryTimeout);
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
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithValidateDefaultParametersChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;validate_default_parameters=false",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = false,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithClientSessionKeepAliveChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;client_session_keep_alive=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = true,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithTimeoutChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;connection_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = 15,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithInsecureModeChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;insecureMode=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = true,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithDisableRetryChanged = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;disableRetry=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = true,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithForceRetryOn404Changed = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;forceRetryOn404=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = true,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueAbove300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=600",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = 600,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueBelow300 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithRetryTimeoutChangedToZero = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;retry_timeout=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = 0,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = 0,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithMaxHttpRetriesChangedToAValueAbove7 = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=10",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
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
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = SFSessionHttpClientProperties.s_maxHttpRetriesDefault
                }
            };
            var propertiesWithMaxHttpRetriesChangedToZero = new PropertiesTestCase()
            {
                conectionString = "account=test;user=test;password=test;maxHttpRetries=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    timeoutInSec = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    insecureMode = false,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.s_retryTimeoutDefault,
                    maxHttpRetries = 0
                }
            };
            return new []
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
