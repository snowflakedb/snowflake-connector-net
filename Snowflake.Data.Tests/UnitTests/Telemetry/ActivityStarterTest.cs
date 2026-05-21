using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Telemetry;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Telemetry;

public sealed class ActivityStarterTest : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly Dictionary<string, Activity> _sessionIdCapturedActivitiesMap = new();

    public ActivityStarterTest()
    {
        _sessionIdCapturedActivitiesMap.Clear();
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivityStarter.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => _sessionIdCapturedActivitiesMap[a.GetTagItem(TelemetryTags.SessionId).ToString()] = a
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener?.Dispose();
    }

    [SFFact]
    public void TestStartActivityCreatesActivityWithSessionTags()
    {
        // Arrange
        var session = CreateSession("sess-123", "WH_TEST", "ROLE_ADMIN", "DB_PROD", true);

        // Act
        using var activity = session.StartActivity("TestOperation");

        // Assert
        Assert.NotNull(activity);
        Assert.Equal("TestOperation", activity.OperationName);
        Assert.Equal(ActivityKind.Client, activity.Kind);
        Assert.Equal("snowflake", activity.GetTagItem(TelemetryTags.DbSystem));
        Assert.Equal("WH_TEST", activity.GetTagItem(TelemetryTags.DbWarehouse));
        Assert.Equal("ROLE_ADMIN", activity.GetTagItem(TelemetryTags.DbRole));
        Assert.Equal("DB_PROD", activity.GetTagItem(TelemetryTags.DbName));
        Assert.Equal(session.sessionId, activity.GetTagItem(TelemetryTags.SessionId));
    }

    [SFFact]
    public void TestStartActivityThrowsOnNullName()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        Assert.Throws<ArgumentException>(() => session.StartActivity(null));
    }

    [SFFact]
    public void TestStartActivityThrowsOnEmptyName()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        Assert.Throws<ArgumentException>(() => session.StartActivity(""));
    }

    [SFFact]
    public void TestSetSuccessSetsOkStatusAndStops()
    {
        // Arrange
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");

        // Act
        activity.SetSuccess();

        // Assert
        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        Assert.Equal("OK", _sessionIdCapturedActivitiesMap[session.sessionId].GetTagItem(TelemetryTags.StatusCode));
    }

    [SFFact]
    public void TestSetExceptionSetsErrorStatusAndAddsEvent()
    {
        // Arrange
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var exception = new AbandonedMutexException("something went wrong");

        // Act
        activity.SetException(exception);

        // Assert
        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        Assert.Equal("ERROR", _sessionIdCapturedActivitiesMap[session.sessionId].GetTagItem(TelemetryTags.StatusCode));

        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.FirstOrDefault(e => e.Name == "exception");
        Assert.NotNull(exceptionEvent);
        Assert.Equal(typeof(AbandonedMutexException).FullName,
            exceptionEvent.Tags.First(t => t.Key == TelemetryTags.Exception).Value);
    }

    [SFFact]
    public void TestSetSuccessOnNullActivityDoesNotThrow()
    {
        ((Activity)null).SetSuccess();
    }

    [SFFact]
    public void TestSetExceptionOnNullActivityDoesNotThrow()
    {
        ((Activity)null).SetException(new Exception("test"));
    }

    [SFFact]
    public void TestSetExceptionWithNullExceptionSetsUnknownError()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");

        activity.SetException(null);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        Assert.Equal("ERROR", _sessionIdCapturedActivitiesMap[session.sessionId].GetTagItem(TelemetryTags.StatusCode));
        Assert.Empty(_sessionIdCapturedActivitiesMap[session.sessionId].Events);
    }

    [SFFact]
    public void TestSetExceptionIncludesErrorCodeForSnowflakeDbException()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var exception = new SnowflakeDbException(SFError.INTERNAL_ERROR, "test failure");

        activity.SetException(exception);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        Assert.Equal(typeof(SnowflakeDbException).FullName,
            exceptionEvent.Tags.First(t => t.Key == TelemetryTags.Exception).Value);
        var errorCode = exceptionEvent.Tags.FirstOrDefault(t => t.Key == TelemetryTags.ExceptionErrorCode).Value;
        Assert.NotNull(errorCode);
        Assert.Equal(exception.ErrorCode.ToString(), errorCode);
    }

    [SFFact]
    public void TestSetExceptionDoesNotIncludeErrorCodeForNonSnowflakeException()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var exception = new InvalidOperationException("plain error");

        activity.SetException(exception);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        Assert.DoesNotContain(exceptionEvent.Tags, t => t.Key == TelemetryTags.ExceptionErrorCode);
    }

    [SFFact]
    public void TestSetExceptionUnwrapsSnowflakeDbExceptionFromAggregateException()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var inner = new SnowflakeDbException(SFError.INTERNAL_ERROR, "nested failure");
        var aggregate = new AggregateException("wrapper", inner);

        activity.SetException(aggregate);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        var errorCode = exceptionEvent.Tags.FirstOrDefault(t => t.Key == TelemetryTags.ExceptionErrorCode).Value;
        Assert.NotNull(errorCode);
        Assert.Equal(inner.ErrorCode.ToString(), errorCode);
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void TestSetExceptionUnwrapsNestedAggregateExceptions(int nestingLevel)
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        Exception snowflakeEx = new SnowflakeDbException(SFError.INTERNAL_ERROR, "deep");
        var rootException = Enumerable.Range(0, nestingLevel).Aggregate(snowflakeEx, (ex, i) => new AggregateException($"{nestingLevel - i} level", ex));

        activity.SetException(rootException);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        var errorCode = exceptionEvent.Tags.FirstOrDefault(t => t.Key == TelemetryTags.ExceptionErrorCode).Value;
        Assert.NotNull(errorCode);
        Assert.Equal(((SnowflakeDbException)snowflakeEx).ErrorCode.ToString(), errorCode);
    }

    [SFFact]
    public void TestSetExceptionDoesNotUnwrapNestedAggregateExceptionsAfterThreshold()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        Exception snowflakeEx = new SnowflakeDbException(SFError.INTERNAL_ERROR, "deep");
        var rootException = Enumerable.Range(0, 5).Aggregate(snowflakeEx, (ex, i) => new AggregateException($"{5 - i} level", ex));

        activity.SetException(rootException);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        var errorCode = exceptionEvent.Tags.FirstOrDefault(t => t.Key == TelemetryTags.ExceptionErrorCode).Value;
        Assert.Null(errorCode);
    }

    [SFFact]
    public void TestSetExceptionDoesNotUnwrapNestedAggregateExceptionsAfterThresholdEvenThoughDepthFirstWouldWork()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var snowflakeEx = new SnowflakeDbException(SFError.INTERNAL_ERROR, "deep");
        var level2 = new AggregateException("level2");
        var level1B = new AggregateException("level1b", snowflakeEx);
        var level1 = new AggregateException("level1", level1B);
        var root = new AggregateException("root", level1, level2); // depth-first distance is 3, but with width-first we exceed threshold.

        activity.SetException(root);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        var errorCode = exceptionEvent.Tags.FirstOrDefault(t => t.Key == TelemetryTags.ExceptionErrorCode).Value;
        Assert.Null(errorCode);
    }

    [SFFact]
    public void TestSetExceptionWithAggregateExceptionWithoutSnowflakeDbException()
    {
        var session = CreateSession("s1", "wh", "role", "db", true);
        var activity = session.StartActivity("Op");
        var aggregate = new AggregateException("wrapper", new AbandonedMutexException("not snowflake"));

        activity.SetException(aggregate);

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var exceptionEvent = _sessionIdCapturedActivitiesMap[session.sessionId].Events.First(e => e.Name == "exception");
        Assert.DoesNotContain(exceptionEvent.Tags, t => t.Key == TelemetryTags.ExceptionErrorCode);
    }

    [SFFact]
    public void TestAddTelemetryEventOnNullActivityDoesNotThrow()
    {
        ((Activity)null).AddTelemetryEvent("SomeEvent");
    }

    [SFFact]
    public void TestAddTelemetryEventAddsNamedEvent()
    {
        var session = CreateSession("s1", "wh", "role", "db", telemetryEnabled: true);
        var activity = session.StartActivity("Op");
        Assert.NotNull(activity);

        activity.AddTelemetryEvent("TestEvent");
        activity.Stop();

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var evt = _sessionIdCapturedActivitiesMap[session.sessionId].Events.FirstOrDefault(e => e.Name == "TestEvent");
        Assert.NotNull(evt);
    }

    [SFFact]
    public void TestAddTelemetryEventWithTagsStoresTagsOnEvent()
    {
        var session = CreateSession("s1", "wh", "role", "db", telemetryEnabled: true);
        var activity = session.StartActivity("Op");
        Assert.NotNull(activity);

        activity.AddTelemetryEvent("StepComplete",
            new KeyValuePair<string, object>("step.name", "validation"),
            new KeyValuePair<string, object>("step.duration_ms", 42));
        activity.Stop();

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var evt = _sessionIdCapturedActivitiesMap[session.sessionId].Events.FirstOrDefault(e => e.Name == "StepComplete");
        Assert.NotNull(evt);
        Assert.Equal("validation", evt.Tags.First(t => t.Key == "step.name").Value);
        Assert.Equal(42, evt.Tags.First(t => t.Key == "step.duration_ms").Value);
    }

    [SFFact]
    public void TestAddTelemetryEventWithNoTagsCreatesEventWithEmptyTags()
    {
        var session = CreateSession("s1", "wh", "role", "db", telemetryEnabled: true);
        var activity = session.StartActivity("Op");
        Assert.NotNull(activity);

        activity.AddTelemetryEvent("SimpleEvent");
        activity.Stop();

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        var evt = _sessionIdCapturedActivitiesMap[session.sessionId].Events.FirstOrDefault(e => e.Name == "SimpleEvent");
        Assert.NotNull(evt);
        Assert.False(evt.Tags.Any());
    }

    [SFFact]
    public void TestAddTelemetryEventWithEmptyNameDoesNotAddEvent()
    {
        var session = CreateSession("s1", "wh", "role", "db", telemetryEnabled: true);
        var activity = session.StartActivity("Op");
        Assert.NotNull(activity);

        activity.AddTelemetryEvent("",
            new KeyValuePair<string, object>("key", "value"));
        activity.AddTelemetryEvent(null,
            new KeyValuePair<string, object>("key", "value"));
        activity.Stop();

        Assert.True(_sessionIdCapturedActivitiesMap.ContainsKey(session.sessionId));
        Assert.Empty(_sessionIdCapturedActivitiesMap[session.sessionId].Events);
    }

    [SFFact]
    public void TestStartActivityOnCommandWithNullConnectionThrows()
    {
        var command = new SnowflakeDbCommand();
        Assert.Throws<ArgumentException>(() => command.StartActivity("Op"));
    }

    [SFFact]
    public void TestStartActivityOnCommandWithClosedConnectionThrows()
    {
        var connection = new SnowflakeDbConnection();
        var command = new SnowflakeDbCommand(connection);
        Assert.Throws<ArgumentException>(() => command.StartActivity("Op"));
    }

    [SFFact]
    public void TestStartActivityOnCommandWithTelemetryDisabledThrows()
    {
        var session = CreateSession("s1", "wh", "role", "db", telemetryEnabled: false);

        var connection = new SnowflakeDbConnection
        {
            SfSession = session,
            _connectionState = ConnectionState.Open
        };
        var command = new SnowflakeDbCommand(connection);

        Assert.Throws<ArgumentException>(() => command.StartActivity("Op"));
    }

    [SFFact]
    public void TestStartActivityOnCommandUsesCustomActivitySource()
    {
        Activity captured = null;
        using var customListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivityStarter.ClientDefinedTelemetrySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => captured = a,
        };
        ActivitySource.AddActivityListener(customListener);

        var session = CreateSession("custom-s1", "WH", "ROLE", "DB", telemetryEnabled: true);

        var connection = new SnowflakeDbConnection
        {
            SfSession = session,
            _connectionState = ConnectionState.Open
        };
        var command = new SnowflakeDbCommand(connection);

        using var activity = command.StartActivity("CustomOp");

        Assert.NotNull(activity);
        Assert.Equal(ActivityStarter.ClientDefinedTelemetrySourceName, activity.Source.Name);
        Assert.Equal("CustomOp", activity.OperationName);
        Assert.Equal(session.sessionId, activity.GetTagItem(TelemetryTags.SessionId));
    }

    private SFSession CreateSession(string sessionIdPrefix, string warehouse, string role, string database, bool telemetryEnabled)
    {
        var sessionId = sessionIdPrefix + Guid.NewGuid().ToString("N");
        var connectionStr = "authenticator=snowflake;account=some_account;user=some_user;"
                            + "password=fake_pwd;"
                            + "db=testDb;role=ANALYST;warehouse=testWarehouse;host=localhost;port=443;scheme=https";
        var mockRest = new Mock<IMockRestRequester>();
        var session = new Mock<SFSession>(connectionStr, new SessionPropertiesContext(), mockRest.Object)
        {
            Object =
            {
                sessionId = sessionId,
                warehouse = warehouse,
                role = role,
                database = database,
                sessionToken = $"token-{sessionId}"
            }
        };
        session.Setup(x => x.IsClientTelemetryEnabled()).Returns(telemetryEnabled);

        return session.Object;
    }
}
