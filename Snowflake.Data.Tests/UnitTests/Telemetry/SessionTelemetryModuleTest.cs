using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Telemetry;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Telemetry;

[Collection(nameof(SessionTelemetryModuleTestFixture))]
public sealed class SessionTelemetryModuleTest
{
    [CollectionDefinition(nameof(SessionTelemetryModuleTestFixture), DisableParallelization =  true)]
    public sealed class SessionTelemetryModuleTestFixture : ICollectionFixture<SessionTelemetryModuleTestFixture>
    { }

    private readonly Mock<IMockRestRequester> _mockRestRequester;

    public SessionTelemetryModuleTest()
    {
        _mockRestRequester = new Mock<IMockRestRequester>();
    }

    [SFFact]
    public void TestDisposeIsIdempotent()
    {
        var session = CreateSession();
        var module = new SessionTelemetryModule(session);

        // Act - double dispose should not throw
        module.Dispose();
        module.Dispose();
    }

    [SFFact]
    public void TestDisposeIsIdempotentWhenBuffer()
    {
        var session = CreateSession();
        var module = new SessionTelemetryModule(session);
        var activity = new Activity("whatever");
        activity.Start();
        module.OnActivityStoppedImpl(activity);
        Assert.Equal(1, ((ISessionTelemetryModule)module).CurrentBufferSize);
        GC.KeepAlive(module);

        // Act - double dispose should not throw
        module.Dispose();
        Assert.Equal(0, ((ISessionTelemetryModule)module).CurrentBufferSize);
        module.Dispose();
    }

    [SFTheory]
    [InlineData(null, null, null, "UNSET", 1)]
    [InlineData(null, null, ActivityStatusCode.Ok, "OK", 1)]
    [InlineData(null, null, ActivityStatusCode.Error, "ERROR", 1)]
    [InlineData("EventA", null, null, "UNSET", 2)]
    [InlineData("EventA", "EventB", null, "UNSET", 3)]
    [InlineData("EventA", "EventB", ActivityStatusCode.Ok, "OK", 3)]
    public async Task TestFlushAsyncConvertsActivityToTelemetryData(string event1, string event2, ActivityStatusCode? status, string expectedStatus, int expectedLogCount)
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) => capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody)
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("TestOp");
        activity.Start();
        if (status.HasValue)
        {
            activity.SetStatus(status.Value);
            activity.SetTag(TelemetryTags.StatusCode, expectedStatus);
        }
        if (event1 != null)
            activity.AddEvent(new ActivityEvent(event1));
        if (event2 != null)
            activity.AddEvent(new ActivityEvent(event2));
        module.OnActivityStoppedImpl(activity);

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        Assert.Equal(expectedLogCount, capturedBody.Logs.Count);
        // First entry is always the synthetic activity event
        Assert.Equal("TestOp", capturedBody.Logs[0].Message[TelemetryField.EventName]);
        foreach (var log in capturedBody.Logs)
        {
            Assert.Equal("client_activity", log.Message[TelemetryField.Type]);
            Assert.Equal(".NET", log.Message[TelemetryField.DriverType]);
            Assert.Equal(expectedStatus, log.Message[TelemetryField.StatusCode]);
            Assert.NotNull(log.Message[TelemetryField.DriverVersion]);
        }
        if (event1 != null)
            Assert.Equal(event1, capturedBody.Logs[1].Message[TelemetryField.EventName]);
        if (event2 != null)
            Assert.Equal(event2, capturedBody.Logs[2].Message[TelemetryField.EventName]);
    }

    [SFFact]
    public async Task TestFlushAsyncSendsMultipleActivities()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) => capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody)
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        foreach (var name in new[] { "Op1", "Op2", "Op3" })
        {
            var a = new Activity(name);
            a.Start();
            module.OnActivityStoppedImpl(a);
        }

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        Assert.Equal(3, capturedBody.Logs.Count);
        Assert.Equal("Op1", capturedBody.Logs[0].Message[TelemetryField.EventName]);
        Assert.Equal("Op2", capturedBody.Logs[1].Message[TelemetryField.EventName]);
        Assert.Equal("Op3", capturedBody.Logs[2].Message[TelemetryField.EventName]);
    }

    [SFFact]
    public async Task TestFlushAsyncSendsMultipleNestedActivities()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) => capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody)
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        using (var a1 = new Activity("Op1"))
        {
            a1.Start();
            using (var a2 = new Activity("Op2"))
            {
                using (var a3 = new Activity("Op3"))
                {
                    a2.Start();
                    a3.Start();
                    module.OnActivityStoppedImpl(a1);
                    module.OnActivityStoppedImpl(a2);
                    module.OnActivityStoppedImpl(a3);
                }
            }
        }

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        Assert.Equal(3, capturedBody.Logs.Count);
        Assert.Equal("Op1", capturedBody.Logs[0].Message[TelemetryField.EventName]);
        Assert.Equal("Op2", capturedBody.Logs[1].Message[TelemetryField.EventName]);
        Assert.Equal("Op3", capturedBody.Logs[2].Message[TelemetryField.EventName]);
    }

    [SFFact]
    public async Task TestFlushAsyncRecordsActivitySourceName()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) => capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody)
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var source = new ActivitySource("MyApp.Instrumentation");
        using var listener = new ActivityListener // listener is needed, otherwise Starting activity may return null
        {
            ShouldListenTo = s => s.Name == "MyApp.Instrumentation",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        var activity = source.StartActivity("CustomSourceOp");
        Assert.NotNull(activity);
        module.OnActivityStoppedImpl(activity);

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        Assert.Equal("MyApp.Instrumentation", capturedBody.Logs.Single().Message[TelemetryField.Source]);
    }

    [SFTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestFlushAsyncDisablesTelemetryAfterFailure(bool throwException)
    {
        // Arrange
        var session = CreateSession();
        if (throwException)
            _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Network error"));
        else
            _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new NullDataResponse { success = false });

        var module = new SessionTelemetryModule(session);
        var activity1 = new Activity("FirstOp");
        activity1.Start();
        module.OnActivityStoppedImpl(activity1);

        // Act — first flush triggers the failure
        await module.FlushAsync(CancellationToken.None);

        // Buffer more data and flush again — should be a no-op
        var activity2 = new Activity("SecondOp");
        activity2.Start();
        module.OnActivityStoppedImpl(activity2);
        await module.FlushAsync(CancellationToken.None);

        // Assert — Post was attempted only once
        _mockRestRequester.Verify(
            r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()),
            Times.Once());

        module.Dispose();
    }

    [SFFact]
    public async Task TestFlushAsyncSendsBufferedDataWithChangedToken()
    {
        var session = CreateSession();
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("whatever");
        activity.Start();
        module.OnActivityStoppedImpl(activity);

        await module.FlushAsync(CancellationToken.None);
        var expectedToken1 = "test-token";
        _mockRestRequester.Verify(x => x.PostAsync<NullDataResponse>(It.Is<IRestRequest>(y => ((SFRestRequest)y).authorizationToken == $"Snowflake Token=\"{expectedToken1}\""), It.IsAny<CancellationToken>()), Times.Once);

        var newToken = "new-token";
        module.UpdateToken(newToken);
        var activity2 = new Activity("whatever 2");
        activity2.Start();
        module.OnActivityStoppedImpl(activity2);
        await module.FlushAsync(CancellationToken.None);
        _mockRestRequester.Verify(x => x.PostAsync<NullDataResponse>(It.Is<IRestRequest>(y => ((SFRestRequest)y).authorizationToken == $"Snowflake Token=\"{newToken}\""), It.IsAny<CancellationToken>()), Times.Once);
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(3)]
    public void TestSyncFlushOnDisposeOnFailure(int disposeCalls)
    {
        // Arrange
        var session = CreateSession();
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Throws(new Exception("Network error"));

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("whatever");
        activity.Start();
        module.OnActivityStoppedImpl(activity);

        // Act - dispose triggers sync Flush which hits the exception
        for (; disposeCalls > 0; disposeCalls--) module.Dispose();

        // Verify Post was attempted once during dispose
        _mockRestRequester.Verify(
            r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()),
            Times.Once());

        Assert.True(((ISessionTelemetryModule)module).IsDisposed);
        Assert.False(((ISessionTelemetryModule)module).IsServiceAvailable);
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(3)]
    public void TestSyncFlushOnDisposeDisablesTelemetryOnNonSuccessResponse(int disposeCalls)
    {
        // Arrange
        var session = CreateSession();
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = false });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("whatever");
        activity.Start();
        module.OnActivityStoppedImpl(activity);

        // Act - dispose calls sync Flush
        for (; disposeCalls > 0; disposeCalls--) module.Dispose();

        // Assert - Post was called once; the non-success disabled further telemetry
        _mockRestRequester.Verify(
            r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()),
            Times.Once());

        Assert.True(((ISessionTelemetryModule)module).IsDisposed);
        Assert.False(((ISessionTelemetryModule)module).IsServiceAvailable);
    }

    [SFFact]
    public async Task TestFlushAsyncWithEmptyBufferDoesNotPost()
    {
        // Arrange
        var session = CreateSession();
        var module = new SessionTelemetryModule(session);

        // Act - no data added
        await module.FlushAsync(CancellationToken.None);

        // Assert
        _mockRestRequester.Verify(
            r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()),
            Times.Never());

        module.Dispose();
    }

    [SFFact]
    public void TestDisposeFlushesBufferedData()
    {
        // Arrange
        var session = CreateSession();
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("whatever");
        activity.Start();
        module.OnActivityStoppedImpl(activity);

        // Act
        module.Dispose();

        // Assert
        _mockRestRequester.Verify(
            r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()),
            Times.Once());
    }

    [SFFact]
    public void TestBufferOverflowFlushesBufferedData()
    {
        // Arrange
        var session = CreateSession();
        var datapointsCounter = 0;
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true }).Callback<IRestRequest>(r =>
            {
                var sfRestRequest = (SFRestRequest)r;
                var jsonBody = (TelemetryRequest)sfRestRequest.jsonBody;
                var logsCount = jsonBody.Logs.Count;
                Interlocked.Add(ref datapointsCounter, logsCount);
            });

        var module = new SessionTelemetryModule(session);

        const int bufferSize = 100;
        const int BuffersToSend = 12;

        var totalPoints = (BuffersToSend + 1) * bufferSize - 1;
        Parallel.For(1, totalPoints, i =>
        {
            var activity = new Activity($"whatever {i}");
            activity.Start();
            module.OnActivityStoppedImpl(activity);
        });

        SpinWait.SpinUntil(() => datapointsCounter >= BuffersToSend * bufferSize, TimeSpan.FromSeconds(10));
        Assert.InRange(datapointsCounter, long.MinValue, totalPoints);
    }

    [SFFact]
    public void TestConcurrentAutoFlushDoesNotFireMultipleFlushes()
    {
        // Arrange
        var session = CreateSession();
        var flushCount = 0;
        var mrse = new ManualResetEventSlim(false);
        var mrse2 = new ManualResetEventSlim(false);
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(() =>
            {
                mrse2.Set();
                Interlocked.Increment(ref flushCount);
                mrse.Wait(TimeSpan.FromSeconds(15));
                return new NullDataResponse { success = true };
            });

        var module = new SessionTelemetryModule(session);

        // Act
        const int ActivitiesCount = 30;
        for (var i = 0; i < ActivitiesCount; i++)
        {
            var activity = new Activity($"op{i}");
            activity.Start();
            module.OnActivityStoppedImpl(activity);
        }
        mrse.Set();

        var lastActivity = new Activity($"op{ActivitiesCount}");
        lastActivity.Start();
        module.OnActivityStoppedImpl(lastActivity);

        // Wait for the flush to complete
        SpinWait.SpinUntil(() => !((ISessionTelemetryModule)module).IsFlushInProgress, TimeSpan.FromSeconds(5));

        // Assert — only one concurrent flush should have fired despite passing the threshold multiple times
        Assert.InRange(flushCount, 0, 2);
    }

    [SFFact(RetriesCount = RetriesCount.Thrice)]
    public async Task TimeShouldFlushPeriodically()
    {
        // Arrange
        var session = CreateSession();
        var logsSent = 0;
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true }).Callback<IRestRequest>(r =>
            {
                var sfRestRequest = (SFRestRequest)r;
                var jsonBody = (TelemetryRequest)sfRestRequest.jsonBody;
                var logsCount = jsonBody.Logs.Count;
                Interlocked.Add(ref logsSent, logsCount);
            });

        var timerSpan = TimeSpan.FromMilliseconds(500);
        const int bufferSize = 100;
        var module = new SessionTelemetryModule(session, bufferSize, timerSpan);

        Parallel.For(0, bufferSize - 1, i =>
        {
            var activity = new Activity($"whatever {i}");
            activity.Start();
            module.OnActivityStoppedImpl(activity);
        });

        await Task.Delay((int)timerSpan.TotalMilliseconds * 10).ConfigureAwait(false);
        Assert.Equal(bufferSize - 1, logsSent);

        var activity = new Activity($"whatever {bufferSize}");
        activity.Start();
        module.OnActivityStoppedImpl(activity);
        await Task.Delay((int)timerSpan.TotalMilliseconds * 10).ConfigureAwait(false);

        Assert.Equal(bufferSize, logsSent);
    }

    [SFFact]
    public async Task TestActivityTagValuesAreMaskedBySecretDetector()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) =>
            {
                capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody;
            })
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("SecretTest");
        activity.Start();
        activity.SetTag("safe.tag", "hello");
        activity.SetTag("credential", "password=fake_pwd");
        module.OnActivityStoppedImpl(activity);

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        var log = capturedBody.Logs.First();
        Assert.Equal("hello", log.Message["tag.safe.tag"]);
        // The password value should be masked by SecretDetector
        Assert.Equal("password=****", log.Message["tag.credential"]);
    }

    [SFFact]
    public async Task TestActivityTagWithNullValueIsSkipped()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) =>
            {
                capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody;
            })
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("NullTagTest");
        activity.Start();
        activity.SetTag("present", "value");
        activity.SetTag("absent", null);
        module.OnActivityStoppedImpl(activity);

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        var log = capturedBody.Logs.First();
        Assert.True(log.Message.ContainsKey("tag.present"));
        Assert.False(log.Message.ContainsKey("tag.absent"));
    }

    [SFFact]
    public async Task TestActivityTagsOnSyntheticEventAndEventTagsOnExplicitEvents()
    {
        // Arrange
        var session = CreateSession();
        TelemetryRequest capturedBody = null;
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .Callback<IRestRequest, CancellationToken>((req, _) =>
            {
                capturedBody = (TelemetryRequest)((SFRestRequest)req).jsonBody;
            })
            .ReturnsAsync(new NullDataResponse { success = true });

        var module = new SessionTelemetryModule(session);
        var activity = new Activity("MyOp");
        activity.Start();
        activity.SetTag("session.tag", "session_value");
        activity.AddEvent(new ActivityEvent("Step1", tags: new ActivityTagsCollection(new[]
        {
            new KeyValuePair<string, object>("event.tag", "event_value"),
        })));
        module.OnActivityStoppedImpl(activity);

        // Act
        await module.FlushAsync(CancellationToken.None);

        // Assert
        Assert.NotNull(capturedBody);
        Assert.Equal(2, capturedBody.Logs.Count);

        // Synthetic event carries activity-level tags
        var syntheticLog = capturedBody.Logs[0];
        Assert.Equal("MyOp", syntheticLog.Message[TelemetryField.EventName]);
        Assert.Equal("session_value", syntheticLog.Message["tag.session.tag"]);
        Assert.False(syntheticLog.Message.ContainsKey("tag.event.tag"));

        // Explicit event carries its own tags
        var eventLog = capturedBody.Logs[1];
        Assert.Equal("Step1", eventLog.Message[TelemetryField.EventName]);
        Assert.Equal("event_value", eventLog.Message["tag.event.tag"]);
        Assert.False(eventLog.Message.ContainsKey("tag.session.tag"));
    }


    [SFTheory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void TestSetFlushSizeThrowsOnInvalidValue(int invalidSize)
    {
        Assert.Throws<ArgumentException>(() => SessionTelemetryModuleFacade.SetFlushSize(invalidSize));
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(500)]
    public void TestSetFlushSizeAcceptsValidValue(int validSize)
    {
        SessionTelemetryModuleFacade.SetFlushSize(validSize);
        SessionTelemetryModuleFacade.SetFlushSize(100);
    }

    [SFTheory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public void TestSetFlushIntervalThrowsOnInvalidValue(int invalidInterval)
    {
        Assert.Throws<ArgumentException>(() => SessionTelemetryModule.SetFlushInterval(invalidInterval));
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(1000)]
    [InlineData(120_000)]
    public void TestSetFlushIntervalAcceptsValidValue(int validInterval)
    {
        SessionTelemetryModule.SetFlushInterval(validInterval);
        SessionTelemetryModuleFacade.SetFlushInterval(1_000 * 60);
    }

    [SFFact]
    public void TestSetFlushSizeAffectsAutoFlushThreshold()
    {
        // Arrange
        var session = CreateSession();
        var flushed = false;
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true })
            .Callback(() => flushed = true);

        SessionTelemetryModuleFacade.SetFlushSize(5);
        try
        {
            var module = new SessionTelemetryModule(session);

            // Act - add exactly 5 items to trigger auto-flush at the new threshold
            for (var i = 0; i < 5; i++)
            {
                var activity = new Activity($"op{i}");
                activity.Start();
                module.OnActivityStoppedImpl(activity);
            }

            SpinWait.SpinUntil(() => flushed, TimeSpan.FromSeconds(5));

            // Assert
            Assert.True(flushed);
            module.Dispose();
        }
        finally
        {
            SessionTelemetryModule.SetFlushSize(100);
        }
    }

    [SFFact]
    public void TestSetFlushIntervalAffectsAutoFlushInterval()
    {
        // Arrange
        var session = CreateSession();
        var flushedCount = 0;
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true })
            .Callback<IRestRequest>(r =>
            {
                var sfRestRequest = (SFRestRequest)r;
                var jsonBody = (TelemetryRequest)sfRestRequest.jsonBody;
                var logsCount = jsonBody.Logs.Count;
                Interlocked.Add(ref flushedCount, logsCount);
            });

        SessionTelemetryModuleFacade.SetFlushInterval(100);
        try
        {
            var module = new SessionTelemetryModule(session);
            for (var i = 0; i < 5; i++)
            {
                var activity = new Activity($"op{i}");
                activity.Start();
                module.OnActivityStoppedImpl(activity);
            }

            var result = SpinWait.SpinUntil(() => flushedCount == 5, TimeSpan.FromSeconds(5));

            // Assert
            Assert.True(result);
            module.Dispose();
        }
        finally
        {
            SessionTelemetryModule.SetFlushInterval(1_000 * 60);
        }
    }

    private SFSession CreateSession()
    {
        var connectionStr = new StringBuilder()
            .Append("authenticator=snowflake;account=some_account;user=some_user;")
            .Append("password=fake_pwd")
            .Append("db=testDb;role=dummyrole;warehouse=test;host=localhost;port=997;scheme=http;")
            .Append("token=test-token")
            .ToString();
        var session = new Mock<SFSession>(connectionStr, new SessionPropertiesContext(), _mockRestRequester.Object)
        {
            Object =
            {
                sessionId = "test-session-id",
                warehouse = "WH",
                role = "ROLE",
                database = "DB",
                sessionToken = "test-token"
            }
        };

        return session.Object;
    }
}
