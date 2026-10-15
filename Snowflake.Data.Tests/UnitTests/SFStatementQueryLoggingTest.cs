using System;
using System.Collections.Generic;
using Moq;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Logger;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SFStatementQueryLoggingTest
{
    private sealed class TestQueryLoggingConfig : QueryLoggingConfig
    {
        public TestQueryLoggingConfig(bool logQueryText, bool logQueryParameters, int maxQueryLength)
            : base(logQueryText, logQueryParameters, maxQueryLength) { }
    }

    private sealed class FakeSecretDetector(string replacementText, string errorStr = null) : ISecretDetector
    {
        public void SetCustomPatterns(string[] customRegex, string[] customMask) { }

        public void ClearCustomPatterns() { }

        public Mask MaskSecrets(string text) => errorStr != null
            ? new Mask(isMasked: true, maskedText: errorStr, errStr: errorStr)
            : new Mask(isMasked: true, maskedText: replacementText, errStr: null);
    }

    private static Mock<SFLogger> CreateInfoEnabledMock()
    {
        var mock = new Mock<SFLogger>();
        mock.Setup(l => l.IsInfoEnabled()).Returns(true);
        return mock;
    }

    private static SFRestRequest CreateRequest(string sqlText, Dictionary<string, BindingDTO> parameterBindings = null)
    {
        return new SFRestRequest
        {
            jsonBody = new QueryRequest
            {
                sqlText = sqlText,
                parameterBindings = parameterBindings
            }
        };
    }

    [SFFact]
    public void TestLogQueryTextWhenEnabled()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        logger.LogQuery(CreateRequest("SELECT 1"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Query [req-123]: SELECT 2")), null), Times.Once);
    }

    [SFFact]
    public void TestNoLogWhenQueryTextDisabled()
    {
        var mock = new Mock<SFLogger>();
        var config = new TestQueryLoggingConfig(logQueryText: false, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        logger.LogQuery(CreateRequest("SELECT 1"), "req-123");

        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestNoLogWithNullSql()
    {
        var mock = new Mock<SFLogger>();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        logger.LogQuery(CreateRequest(null), "req-123");

        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestNoLogWhenRequestBodyIsNotQueryRequest()
    {
        var mock = new Mock<SFLogger>();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        var request = new SFRestRequest { jsonBody = new object() };
        logger.LogQuery(request, "req-123");

        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
        mock.Verify(l => l.Error(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestQueryTextIsMaskedBeforeLogging()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var detector = new FakeSecretDetector(replacementText: "SELECT ****");
        var logger = new QuerySFLogger(config, mock.Object, detector);

        logger.LogQuery(CreateRequest("SELECT password='secret123'"), "req-1");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains("Query [req-1]: SELECT ****") && !s.Contains("secret123")), null), Times.Once);
    }

    [SFFact]
    public void TestQueryTextLoggedUnchangedWhenNoSecretsFound()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var detector = new FakeSecretDetector("SELECT 1");
        var logger = new QuerySFLogger(config, mock.Object, detector);

        logger.LogQuery(CreateRequest("SELECT 1"), "req-1");

        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Query [req-1]: SELECT 1")), null), Times.Once);
        mock.Verify(l => l.Error(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestQueryTextNotLoggedWhenMaskingFails()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var detector = new FakeSecretDetector(null, errorStr: "regex timeout");
        var logger = new QuerySFLogger(config, mock.Object, detector);

        logger.LogQuery(CreateRequest("SELECT secret_data"), "req-1");

        mock.Verify(l => l.Error(It.Is<string>(s => s.Contains("regex timeout")), null), Times.Once);
        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestParametersNotLoggedWhenMaskingFails()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var detector = new FakeSecretDetector(null, errorStr: "regex timeout");
        var logger = new QuerySFLogger(config, mock.Object, detector);
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "secret-value") }
        };

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-1");

        mock.Verify(l => l.Error(It.Is<string>(s => s.Contains("regex timeout")), null), Times.Exactly(2));
        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestParameterValuesAreMaskedBeforeLogging()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var detector = new FakeSecretDetector(replacementText: "****");
        var logger = new QuerySFLogger(config, mock.Object, detector);

        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "my-api-key-12345") }
        };

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-1");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains("Parameters [req-1]:") && s.Contains("****") && !s.Contains("my-api-key")), null), Times.Once);
    }

    [SFFact]
    public void TestLogParametersWhenEnabled()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "john") },
            { "2", new BindingDTO("FIXED", 37) }
        };

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains("Parameters [req-123]:") && s.Contains("jack 21")), null), Times.Once);
    }

    [SFFact]
    public void TestNoLogWhenParametersDisabled()
    {
        var mock = new Mock<SFLogger>();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "john") }
        };
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Parameters")), null), Times.Never);
    }

    [SFFact]
    public void TestNoLogWithEmptyBindings()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        logger.LogQuery(CreateRequest("SELECT 1", new Dictionary<string, BindingDTO>()), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Query [req-123]")), null), Times.Once);
        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Parameters")), null), Times.Never);
    }

    [SFFact]
    public void TestNullBindingValueLogged()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(null));
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "jack 21") }
        };

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains("Parameters [req-123]:")), null), Times.Once);
    }

    [SFTheory]
    [InlineData(200)]
    [InlineData(QueryLoggingConfig.MaxQueryLengthPropertyMinValue)]
    public void TestLogQueryTextWithTruncation(int queryLength)
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: queryLength);
        var longQuery = "A" + new string('B', queryLength + 1);
        var expectedQuery =
            queryLength == 200
                ? "Query [req-123]: ABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB... [truncated from 202 to 200 chars]"
                : "Query [req-123]: ABBBBBBBBBBBBBB... [truncated from 52 to 50 chars]";
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(longQuery));

        logger.LogQuery(CreateRequest("jack 21"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Equals(expectedQuery, StringComparison.Ordinal)), null), Times.Once);
    }

    [SFFact]
    public void TestLogQueryTextNoTruncationWhenUnderLimit()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: 100);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        logger.LogQuery(CreateRequest("SELECT 37"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains("Query [req-123]: jack 21") && !s.Contains("truncated")), null), Times.Once);
    }

    [SFFact]
    public void TestTruncationAppliesAfterMasking()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: 50);
        var expandedMaskedText = "SELECT " + new string('*', 200) + " FROM secrets";
        var detector = new FakeSecretDetector(replacementText: expandedMaskedText);
        var logger = new QuerySFLogger(config, mock.Object, detector);
        const string ExpectedQuery = "Query [req-1]: SELECT *******... [truncated from 220 to 50 chars]";

        logger.LogQuery(CreateRequest("SELECT token FROM secrets"), "req-1");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Equals(ExpectedQuery, StringComparison.Ordinal)), null), Times.Once);
    }

    [SFFact]
    public void TestLogParametersWithTruncation()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: 100);
        var longValue = new string('X', 200);
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "jack 21") }
        };
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(longValue));
        const string ExpectedParameters = "Parameters [req-123]: [XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX... [truncated from 200 to 100 chars]]";

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Equals(ExpectedParameters, StringComparison.Ordinal)), null), Times.Once);
    }

    [SFFact]
    public void TestNoTruncationWhenMaxLengthDisabled()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var longQuery = new string('A', 5000);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(longQuery));

        logger.LogQuery(CreateRequest("jack 21"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s.Contains(longQuery) && !s.Contains("truncated")), null), Times.Once);
    }

    [SFFact]
    public void TestNoQueryTextLogWhenInfoLevelDisabled()
    {
        var mock = new Mock<SFLogger>();
        mock.Setup(l => l.IsInfoEnabled()).Returns(false);
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        logger.LogQuery(CreateRequest("SELECT 1"), "req-123");

        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestNoParameterLogWhenInfoLevelDisabled()
    {
        var mock = new Mock<SFLogger>();
        mock.Setup(l => l.IsInfoEnabled()).Returns(false);
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "john") }
        };
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestQueryTextLoggedWhenInfoLevelEnabled()
    {
        var mock = new Mock<SFLogger>();
        mock.Setup(l => l.IsInfoEnabled()).Returns(true);
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("SELECT 2"));

        logger.LogQuery(CreateRequest("SELECT 1"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s => s.Contains("Query [req-123]: SELECT 2")), null), Times.Once);
    }

    [SFFact]
    public void TestNewlinesInQueryTextAreSanitized()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: false, maxQueryLength: -1);
        var multiLineQuery = "SELECT *\nFROM users\r\nWHERE id = 1";
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(multiLineQuery));

        logger.LogQuery(CreateRequest("jack 21"), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s == @"Query [req-123]: SELECT *\nFROM users\nWHERE id = 1"), null), Times.Once);
    }

    [SFFact]
    public void TestNewlinesInParameterValuesAreSanitized()
    {
        var mock = CreateInfoEnabledMock();
        var config = new TestQueryLoggingConfig(logQueryText: true, logQueryParameters: true, maxQueryLength: -1);
        var multiLineValue = "line1\nline2\r\nline3";
        var bindings = new Dictionary<string, BindingDTO>
        {
            { "1", new BindingDTO("TEXT", "jack 21") }
        };
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector(multiLineValue));

        logger.LogQuery(CreateRequest("SELECT ?", bindings), "req-123");

        mock.Verify(l => l.Info(It.Is<string>(s =>
            s == @"Parameters [req-123]: [line1\nline2\nline3]"), null), Times.Once);
    }

    [SFFact]
    public void TestDelegatesLogLevelChecksToDecorated()
    {
        var mock = new Mock<SFLogger>();
        mock.Setup(l => l.IsDebugEnabled()).Returns(true);
        mock.Setup(l => l.IsInfoEnabled()).Returns(false);
        var config = new TestQueryLoggingConfig(logQueryText: false, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        Assert.True(logger.IsDebugEnabled());
        Assert.False(logger.IsInfoEnabled());
    }

    [SFFact]
    public void TestDelegatesLogCallsToDecorated()
    {
        var mock = new Mock<SFLogger>();
        var config = new TestQueryLoggingConfig(logQueryText: false, logQueryParameters: false, maxQueryLength: -1);
        var logger = new QuerySFLogger(config, mock.Object, new FakeSecretDetector("jack 21"));

        logger.Debug("debug msg");
        logger.Warn("warn msg");

        mock.Verify(l => l.Debug("debug msg", null), Times.Once);
        mock.Verify(l => l.Warn("warn msg", null), Times.Once);
    }
}
