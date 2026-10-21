using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public class SFHttpClientPropertiesTest
{
    [SFTheory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    public void TestSettingConnectionLimitProperty(int expectedConnectionLimit)
    {
        // arrange
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;SERVICE_POINT_CONNECTION_LIMIT={expectedConnectionLimit}";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(expectedConnectionLimit, extractedProperties._servicePointConnectionLimit);
    }

    [SFFact]
    public void TestSettingConnectionLimitPropertyToLessThan1()
    {
        // arrange
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;SERVICE_POINT_CONNECTION_LIMIT={0}";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(SFSessionHttpClientProperties.DefaultConnectionLimit, extractedProperties._servicePointConnectionLimit);
    }

    [SFFact]
    public void TestSettingConnectionLimitPropertyToGreaterThanMaxConnectionLimit()
    {
        // arrange
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;SERVICE_POINT_CONNECTION_LIMIT={SFSessionHttpClientProperties.MaxConnectionLimit + 1}";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(SFSessionHttpClientProperties.DefaultConnectionLimit, extractedProperties._servicePointConnectionLimit);
    }

    [SFFact]
    public void TestSettingConnectionLimitPropertyToNoValue()
    {
        // arrange
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;SERVICE_POINT_CONNECTION_LIMIT=";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(SFSessionHttpClientProperties.DefaultConnectionLimit, extractedProperties._servicePointConnectionLimit);
    }

    [SFTheory]
    [InlineData("abc")]
    [InlineData("1.5")]
    [InlineData("true")]
    [InlineData("-2.3")]
    [InlineData("null")]
    public void TestThrowsExceptionWhenSettingConnectionLimitPropertyToNonIntegerValue(string nonIntegerValue)
    {
        // arrange
        var parameterName = "SERVICE_POINT_CONNECTION_LIMIT";
        var expectedErrorMessage = $"Error: Invalid parameter value  for {parameterName}";
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;{parameterName}={nonIntegerValue}";

        // act
        var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

        // assert
        Assert.Equal(SFError.INVALID_CONNECTION_PARAMETER_VALUE.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        Assert.Contains(expectedErrorMessage, thrown.Message);
    }

    [SFTheory]
    [InlineData(100)]
    [InlineData(20971520)]
    public void TestValidCrlDownloadMaxSize(long validMaxSize)
    {
        // arrange
        var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;CRLDOWNLOADMAXSIZE={validMaxSize}";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);
        var config = extractedProperties.BuildHttpClientConfig("default");

        // assert
        Assert.Equal(validMaxSize, config.CrlDownloadMaxSize);
    }

    [SFFact]
    public void TestBuildHttpClientConfig()
    {
        // arrange
        var proxyProperties = new SFSessionHttpClientProxyPropertiesForTests("myhost", "8080", "localhost", "proxypass", "proxyuser");
        var retryProperties = new SFSessionRetryProperties(
            ConnectionTimeout: SFSessionHttpClientProperties.DefaultRetryTimeout,
            DisableRetry: true,
            ForceRetryOn404: true,
            RetryTimeout: SFSessionHttpClientProperties.DefaultRetryTimeout,
            MaxHttpRetries: 10,
            IncludeRetryReason: true);
        var crlProperties = new SFSessionCRLProperties(
            CertRevocationCheckMode: CertRevocationCheckMode.Enabled,
            EnableCrlDiskCaching: false,
            EnableCrlInMemoryCaching: false,
            AllowCertificatesWithoutCrlUrl: true,
            CRLDownloadTimeout: 10,
            CRLDownloadMaxSize: 20971520);
        var httpProps = new SFSessionHttpClientPropertiesForTests(
            retryProperties: retryProperties,
            proxyProperties: proxyProperties,
            crlProperties: crlProperties);

        // act
        var config = httpProps.BuildHttpClientConfig("default");

        // assert
        Assert.Equal("myhost", config.ProxyHost);
        Assert.Equal("8080", config.ProxyPort);
        Assert.Equal("proxyuser", config.ProxyUser);
        Assert.Equal("proxypass", config.ProxyPassword);
        Assert.Equal("localhost", config.NoProxyList);
        Assert.True(config.DisableRetry);
        Assert.True(config.ForceRetryOn404);
        Assert.Equal(10, config.MaxHttpRetries);
        Assert.Equal(CertRevocationCheckMode.Enabled, config.CertRevocationCheckMode);
        Assert.False(config.EnableCRLDiskCaching);
        Assert.False(config.EnableCRLInMemoryCaching);
        Assert.True(config.AllowCertificatesWithoutCrlUrl);
    }

    [SFTheory]
    [InlineData("enabled", true, false)]
    [InlineData("disabled", false, false)]
    [InlineData("advisory", true, false)]
    [InlineData("native", false, true)]
    public void TestIsCustomCrlCheckConfigured(string certCheckMode, bool expectedCustomCrlCheck, bool expectedDotnetCrlCheck)
    {
        // arrange
        var mode = (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode), certCheckMode, true);
        var crlProperties = new SFSessionCRLProperties(
            CertRevocationCheckMode: mode,
            EnableCrlDiskCaching: true,
            EnableCrlInMemoryCaching: true,
            AllowCertificatesWithoutCrlUrl: false,
            CRLDownloadTimeout: 10,
            CRLDownloadMaxSize: 20971520);
        var httpProps = new SFSessionHttpClientPropertiesForTests(crlProperties: crlProperties);

        // act
        var config = httpProps.BuildHttpClientConfig("default");

        // assert
        Assert.Equal(expectedCustomCrlCheck, config.IsCustomCrlCheckConfigured());
        Assert.Equal(expectedDotnetCrlCheck, config.IsDotnetCrlCheckEnabled());
    }

    [SFTheory]
    [InlineData("account=test;user=test;password=test;minTls=tls13;maxTls=tls13", "tls13", "tls13", true)]
    [InlineData("account=test;user=test;password=test;minTls=tls12;maxTls=tls13", "tls12", "tls13", true)]
    [InlineData("account=test;user=test;password=test;minTls=tls12;maxTls=tls12", "tls12", "tls12", true)]
    [InlineData("account=test;user=test;password=test;minTls=tls13", "tls13", "tls13", true)]
    [InlineData("account=test;user=test;password=test", "tls12", "tls13", false)]
    public void TestSslProperties(string connectionString, string expectedMinTls, string expectedMaxTls, bool expectedExplicit)
    {
        // arrange
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(expectedMinTls, extractedProperties._tlsProperties.MinTlsProtocol);
        Assert.Equal(expectedMaxTls, extractedProperties._tlsProperties.MaxTlsProtocol);
        Assert.Equal(expectedExplicit, extractedProperties._tlsProperties.TlsProtocolsExplicitlyRequested);
    }

    [SFTheory]
    // Whatever was requested, the effective protocols always carry the defaults, so the driver
    // keeps pinning TLS 1.2 as a floor even when nothing was asked for.
    [InlineData("account=test;user=test;password=test", SslProtocols.Tls12, SslProtocolsExtensions.Tls13)]
    [InlineData("account=test;user=test;password=test;minTls=tls13", SslProtocolsExtensions.Tls13, SslProtocolsExtensions.Tls13)]
    [InlineData("account=test;user=test;password=test;maxTls=tls12", SslProtocols.Tls12, SslProtocols.Tls12)]
    public void TestEffectiveTlsProtocolsAlwaysResolveDefaults(string connectionString, SslProtocols expectedMin, SslProtocols expectedMax)
    {
        // arrange, act
        var config = SFSessionHttpClientProperties
            .ExtractAndValidate(SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()))
            .BuildHttpClientConfig("");

        // assert
        Assert.Equal(expectedMin, config.MinTlsProtocol);
        Assert.Equal(expectedMax, config.MaxTlsProtocol);
    }

    [SFTheory]
    [InlineData("account=test;user=test;password=test", false)]
    [InlineData("account=test;user=test;password=test;minTls=tls13", true)]
    [InlineData("account=test;user=test;password=test;maxTls=tls13", true)]
    [InlineData("account=test;user=test;password=test;minTls=tls12;maxTls=tls13", true)]
    public void TestTlsProtocolsAreRecognizedAsRequestedOnlyWhenInConnectionString(string connectionString, bool expectedRequested)
    {
        // arrange, act - MINTLS/MAXTLS always end up populated by their defaults, so only the
        // connection string can tell a request to restrict TLS apart from the driver's defaults
        var config = SFSessionHttpClientProperties
            .ExtractAndValidate(SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()))
            .BuildHttpClientConfig("");

        // assert
        Assert.Equal(expectedRequested, config.TlsProtocolsExplicitlyRequested);
    }

    [SFFact]
    public void TestSslPropertiesFailure()
    {
        // act & assert
        var exception = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;minTls=tls13;maxTls=tls12", new SessionPropertiesContext()));
        Assert.Contains("Connection string is invalid: Parameter MINTLS value cannot be higher than MAXTLS value.", exception.Message);
    }

    [SFFact]
    public void TestSslInvalidPropertyFailure()
    {
        // act & assert
        var exception = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;minTls=tls11;maxTls=tls11", new SessionPropertiesContext()));
        Assert.Contains("Connection string is invalid: Parameter MINTLS should have one of the following values: TLS12, TLS13.", exception.Message);
    }

    [SFTheory, MemberData(nameof(PropertiesProvider))]
    public void TestExtractProperties(PropertiesTestCase testCase)
    {
        // arrange
        var properties = SFSessionProperties.ParseConnectionString(testCase.connectionString, new SessionPropertiesContext());

        // act
        var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);

        // assert
        Assert.Equal(testCase.validateDefaultParameters, extractedProperties._validateDefaultParameters);
        Assert.Equal(testCase.clientSessionKeepAlive, extractedProperties._clientSessionKeepAlive);
        Assert.Equal(testCase.connectionTimeout, extractedProperties._retryProperties.ConnectionTimeout);
        Assert.Equal(testCase.expectedClientStoreTemporaryCredential, extractedProperties._clientStoreTemporaryCredential);
        Assert.Equal(testCase.certRevocationCheckMode, extractedProperties._crlProperties.CertRevocationCheckMode);
        Assert.Equal(testCase.enableCrlDiskCaching, extractedProperties._crlProperties.EnableCrlDiskCaching);
        Assert.Equal(testCase.enableCrlInMemoryCaching, extractedProperties._crlProperties.EnableCrlInMemoryCaching);
        Assert.Equal(testCase.allowCertificatesWithoutCrlUrl, extractedProperties._crlProperties.AllowCertificatesWithoutCrlUrl);
        Assert.Equal(testCase.disableRetry, extractedProperties._retryProperties.DisableRetry);
        Assert.Equal(testCase.forceRetryOn404, extractedProperties._retryProperties.ForceRetryOn404);
        Assert.Equal(testCase.retryTimeout, extractedProperties._retryProperties.RetryTimeout);
        Assert.Equal(testCase.maxHttpRetries, extractedProperties._retryProperties.MaxHttpRetries);
    }

    public static IEnumerable<object[]> PropertiesProvider()
    {
        var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        var defaultProperties = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test",
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithValidateDefaultParametersChanged = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;validate_default_parameters=false",
            validateDefaultParameters = false,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithClientSessionKeepAliveChanged = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;client_session_keep_alive=true",
            clientSessionKeepAlive = true,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithTimeoutChanged = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;connection_timeout=15",
            connectionTimeout = TimeSpan.FromSeconds(15),
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithCertRevocationConfigEnabled = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;certRevocationCheckMode=enabled;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=true",
            certRevocationCheckMode = CertRevocationCheckMode.Enabled,
            enableCrlDiskCaching = false,
            enableCrlInMemoryCaching = false,
            allowCertificatesWithoutCrlUrl = true,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithCertRevocationConfigAdvisory = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;certRevocationCheckMode=advisory;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=false",
            certRevocationCheckMode = CertRevocationCheckMode.Advisory,
            enableCrlDiskCaching = false,
            enableCrlInMemoryCaching = false,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithCertRevocationConfigViaDotNet = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;certRevocationCheckMode=native",
            certRevocationCheckMode = CertRevocationCheckMode.Native,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithDisableRetryChanged = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;disableRetry=true",
            disableRetry = true,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithForceRetryOn404Changed = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;forceRetryOn404=true",
            forceRetryOn404 = true,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithRetryTimeoutChangedToAValueAbove300 = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;retry_timeout=600",
            retryTimeout = TimeSpan.FromSeconds(600),
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithRetryTimeoutChangedToAValueBelow300 = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;retry_timeout=15",
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithRetryTimeoutChangedToZero = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;retry_timeout=0",
            connectionTimeout = SFSessionHttpClientProperties.DefaultConnectionTimeout,
            retryTimeout = TimeoutHelper.Infinity(),
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithMaxHttpRetriesChangedToAValueAbove7 = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;maxHttpRetries=10",
            maxHttpRetries = 10,
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithMaxHttpRetriesChangedToAValueBelow7 = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;maxHttpRetries=5",
            expectedClientStoreTemporaryCredential = isWindows
        };
        var propertiesWithMaxHttpRetriesChangedToZero = new PropertiesTestCase
        {
            connectionString = "account=test;user=test;password=test;maxHttpRetries=0",
            maxHttpRetries = 0,
            expectedClientStoreTemporaryCredential = isWindows
        };
        return new PropertiesTestCase[]
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
        }.Select(tc => new object[] { tc });
    }

    [SFFact]
    public void TestExtractorDelegatesToSubExtractors()
    {
        // arrange
        var connectionString = "ACCOUNT=account;USER=test;PASSWORD=test";
        var props = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        var expectedProxy = new SFSessionHttpClientProxyPropertiesForTests();
        var expectedRetry = new SFSessionRetryProperties(
            ConnectionTimeout: TimeSpan.FromSeconds(300),
            DisableRetry: true,
            ForceRetryOn404: true,
            RetryTimeout: TimeSpan.FromSeconds(600),
            MaxHttpRetries: 10,
            IncludeRetryReason: false);
        var expectedCrl = new SFSessionCRLProperties(
            CertRevocationCheckMode: CertRevocationCheckMode.Advisory,
            EnableCrlDiskCaching: false,
            EnableCrlInMemoryCaching: false,
            AllowCertificatesWithoutCrlUrl: true,
            CRLDownloadTimeout: 42,
            CRLDownloadMaxSize: 999);
        var expectedTls = new SFSessionTLSProperties(
            MinTlsProtocol: "tls13",
            MaxTlsProtocol: "tls13",
            TlsProtocolsExplicitlyRequested: true,
            TlsCipherPolicy: null);

        var mockProxy = new Mock<SFSessionHttpClientProxyProperties.IExtractor>();
        mockProxy.Setup(x => x.ExtractProperties(It.IsAny<SFSessionProperties>())).Returns(expectedProxy);

        var mockRetry = new Mock<SFSessionRetryProperties.IExtractor>();
        mockRetry.Setup(x => x.ExtractProperties(It.IsAny<SFSessionProperties>(), It.IsAny<SessionPropertiesWithDefaultValuesExtractor>())).Returns(expectedRetry);

        var mockCrl = new Mock<SFSessionCRLProperties.IExtractor>();
        mockCrl.Setup(x => x.ExtractProperties(It.IsAny<SFSessionProperties>())).Returns(expectedCrl);

        var mockTls = new Mock<SFSessionTLSProperties.IExtractor>();
        mockTls.Setup(x => x.ExtractProperties(It.IsAny<SFSessionProperties>())).Returns(expectedTls);

        var extractor = new SFSessionHttpClientProperties.Extractor(
            mockProxy.Object,
            mockRetry.Object,
            mockCrl.Object,
            mockTls.Object);

        // act
        var result = extractor.ExtractProperties(props);

        // assert - verify delegation
        mockProxy.Verify(x => x.ExtractProperties(props), Times.Once);
        mockRetry.Verify(x => x.ExtractProperties(props, It.IsAny<SessionPropertiesWithDefaultValuesExtractor>()), Times.Once);
        mockCrl.Verify(x => x.ExtractProperties(props), Times.Once);
        mockTls.Verify(x => x.ExtractProperties(props), Times.Once);

        // assert - verify values passed through
        Assert.Same(expectedProxy, result._proxyProperties);
        Assert.Equal(expectedRetry, result._retryProperties);
        Assert.Equal(expectedCrl, result._crlProperties);
        Assert.Equal(expectedTls, result._tlsProperties);
    }

    public class PropertiesTestCase
    {
        internal string connectionString;
        internal bool validateDefaultParameters = true;
        internal bool clientSessionKeepAlive;
        internal TimeSpan connectionTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout;
        internal bool disableRetry;
        internal bool forceRetryOn404;
        internal TimeSpan retryTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout;
        internal int maxHttpRetries = SFSessionHttpClientProperties.DefaultMaxHttpRetries;
        internal bool expectedClientStoreTemporaryCredential;
        internal CertRevocationCheckMode certRevocationCheckMode = CertRevocationCheckMode.Disabled;
        internal bool enableCrlDiskCaching = true;
        internal bool enableCrlInMemoryCaching = true;
        internal bool allowCertificatesWithoutCrlUrl;
    }
}
