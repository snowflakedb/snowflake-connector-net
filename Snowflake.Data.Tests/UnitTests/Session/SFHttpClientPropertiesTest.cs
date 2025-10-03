using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Revocation;
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
                _certRevocationCheckMode = CertRevocationCheckMode.Enabled,
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
            Assert.AreEqual(properties._certRevocationCheckMode, config.CertRevocationCheckMode);
            Assert.AreEqual(properties._enableCrlDiskCaching, config.EnableCRLDiskCaching);
            Assert.AreEqual(properties._enableCrlInMemoryCaching, config.EnableCRLInMemoryCaching);
            Assert.AreEqual(properties._allowCertificatesWithoutCrlUrl, config.AllowCertificatesWithoutCrlUrl);
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
        [TestCase("enabled", true, false)]
        [TestCase("disabled", false, false)]
        [TestCase("advisory", true, false)]
        [TestCase("native", false, true)]
        public void TestIsCustomCrlCheckConfigured(string certCheckMode, bool expectedCustomCrlCheck, bool expectedDotnetCrlCheck)
        {
            // arrange
            var properties = RandomSFSessionHttpClientProperties();
            properties._certRevocationCheckMode = (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode), certCheckMode, true);
            var config = properties.BuildHttpClientConfig();

            // act
            var isCustomCrlCheck = config.IsCustomCrlCheckConfigured();
            var isDotnetCrlCheck = config.IsDotnetCrlCheckEnabled();

            // assert
            Assert.AreEqual(expectedCustomCrlCheck, isCustomCrlCheck);
            Assert.AreEqual(expectedDotnetCrlCheck, isDotnetCrlCheck);
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
            var randomCertRevocationCheckMode = TestDataGenarator.GetRandomEnumValue<CertRevocationCheckMode>();
            return new SFSessionHttpClientProperties()
            {
                validateDefaultParameters = TestDataGenarator.NextBool(),
                clientSessionKeepAlive = TestDataGenarator.NextBool(),
                connectionTimeout = TimeSpan.FromSeconds(TestDataGenarator.NextInt(30, 151)),
                disableRetry = TestDataGenarator.NextBool(),
                forceRetryOn404 = TestDataGenarator.NextBool(),
                retryTimeout = TimeSpan.FromSeconds(TestDataGenarator.NextInt(300, 600)),
                maxHttpRetries = TestDataGenarator.NextInt(0, 15),
                proxyProperties = proxyProperties,
                _certRevocationCheckMode = randomCertRevocationCheckMode,
                _enableCrlDiskCaching = TestDataGenarator.NextBool(),
                _enableCrlInMemoryCaching = TestDataGenarator.NextBool(),
                _allowCertificatesWithoutCrlUrl = TestDataGenarator.NextBool()
            };
        }

        [Test]
        [TestCase("account=test;user=test;password=test;minTls=tls13;maxTls=tls13", "tls13", "tls13")]
        [TestCase("account=test;user=test;password=test;minTls=tls12;maxTls=tls13", "tls12", "tls13")]
        [TestCase("account=test;user=test;password=test;minTls=tls12;maxTls=tls12", "tls12", "tls12")]
        [TestCase("account=test;user=test;password=test", "tls12", "tls13")]
        public void TestSslProperties(string connectionString, string expectedMinTls, string expectedMaxTls)
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());
            // act
            var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);
            // assert
            Assert.AreEqual(extractedProperties._minTlsProtocol, expectedMinTls);
            Assert.AreEqual(extractedProperties._maxTlsProtocol, expectedMaxTls);
        }

        [Test]
        public void TestSslPropertiesFailure()
        {
            // act & assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;minTls=tls13;maxTls=tls12", new SessionPropertiesContext()));
            Assert.That(exception.Message.Contains("Connection string is invalid: Parameter MINTLS value cannot be higher than MAXTLS value."));
        }

        [Test]
        public void TestSslInvalidPropertyFailure()
        {
            // act & assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;minTls=tls11;maxTls=tls11", new SessionPropertiesContext()));
            Assert.That(exception.Message.Contains("Connection string is invalid: Parameter MINTLS should have one of the following values: TLS12, TLS13."));
        }

        [Test, TestCaseSource(nameof(PropertiesProvider))]
        public void TestExtractProperties(PropertiesTestCase testCase)
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString(testCase.connectionString, new SessionPropertiesContext());
            var proxyProperties = new SFSessionHttpClientProxyProperties();

            // act
            var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

            // assert
            Assert.AreEqual(testCase.expectedProperties.validateDefaultParameters, extractedProperties.validateDefaultParameters);
            Assert.AreEqual(testCase.expectedProperties.clientSessionKeepAlive, extractedProperties.clientSessionKeepAlive);
            Assert.AreEqual(testCase.expectedProperties.connectionTimeout, extractedProperties.connectionTimeout);
            Assert.AreEqual(testCase.expectedProperties._clientStoreTemporaryCredential, extractedProperties._clientStoreTemporaryCredential);
            Assert.AreEqual(testCase.expectedProperties._certRevocationCheckMode, extractedProperties._certRevocationCheckMode);
            Assert.AreEqual(testCase.expectedProperties._enableCrlDiskCaching, extractedProperties._enableCrlDiskCaching);
            Assert.AreEqual(testCase.expectedProperties._enableCrlInMemoryCaching, extractedProperties._enableCrlInMemoryCaching);
            Assert.AreEqual(testCase.expectedProperties._allowCertificatesWithoutCrlUrl, extractedProperties._allowCertificatesWithoutCrlUrl);
            Assert.AreEqual(testCase.expectedProperties.disableRetry, extractedProperties.disableRetry);
            Assert.AreEqual(testCase.expectedProperties.forceRetryOn404, extractedProperties.forceRetryOn404);
            Assert.AreEqual(testCase.expectedProperties.retryTimeout, extractedProperties.retryTimeout);
            Assert.AreEqual(testCase.expectedProperties.maxHttpRetries, extractedProperties.maxHttpRetries);
            Assert.NotNull(proxyProperties);
        }

        public static IEnumerable<PropertiesTestCase> PropertiesProvider()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var defaultProperties = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithValidateDefaultParametersChanged = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;validate_default_parameters=false",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = false,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithClientSessionKeepAliveChanged = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;client_session_keep_alive=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = true,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithTimeoutChanged = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;connection_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = TimeSpan.FromSeconds(15),
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithCertRevocationConfigEnabled = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;certRevocationCheckMode=enabled;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Enabled,
                    _enableCrlDiskCaching = false,
                    _enableCrlInMemoryCaching = false,
                    _allowCertificatesWithoutCrlUrl = true

                }
            };
            var propertiesWithCertRevocationConfigAdvisory = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;certRevocationCheckMode=advisory;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=false",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Advisory,
                    _enableCrlDiskCaching = false,
                    _enableCrlInMemoryCaching = false,
                    _allowCertificatesWithoutCrlUrl = false

                }
            };
            var propertiesWithCertRevocationConfigViaDotNet = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;certRevocationCheckMode=native",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Native,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false

                }
            };
            var propertiesWithDisableRetryChanged = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;disableRetry=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = true,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithForceRetryOn404Changed = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;forceRetryOn404=true",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = true,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueAbove300 = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;retry_timeout=600",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = TimeSpan.FromSeconds(600),
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithRetryTimeoutChangedToAValueBelow300 = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;retry_timeout=15",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithRetryTimeoutChangedToZero = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;retry_timeout=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultConnectionTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = TimeoutHelper.Infinity(),
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithMaxHttpRetriesChangedToAValueAbove7 = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;maxHttpRetries=10",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = 10,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithMaxHttpRetriesChangedToAValueBelow7 = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;maxHttpRetries=5",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            var propertiesWithMaxHttpRetriesChangedToZero = new PropertiesTestCase()
            {
                connectionString = "account=test;user=test;password=test;maxHttpRetries=0",
                expectedProperties = new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = true,
                    clientSessionKeepAlive = false,
                    connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    disableRetry = false,
                    forceRetryOn404 = false,
                    retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout,
                    maxHttpRetries = 0,
                    _clientStoreTemporaryCredential = isWindows,
                    _certRevocationCheckMode = CertRevocationCheckMode.Disabled,
                    _enableCrlDiskCaching = true,
                    _enableCrlInMemoryCaching = true,
                    _allowCertificatesWithoutCrlUrl = false
                }
            };
            return new[]
            {
                defaultProperties,
                propertiesWithValidateDefaultParametersChanged,
                propertiesWithClientSessionKeepAliveChanged,
                propertiesWithTimeoutChanged,
                propertiesWithCertRevocationConfigEnabled,
                propertiesWithCertRevocationConfigAdvisory,
                propertiesWithCertRevocationConfigViaDotNet,
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
            internal string connectionString;
            internal SFSessionHttpClientProperties expectedProperties;
        }
    }
}
