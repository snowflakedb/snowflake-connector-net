using System;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Telemetry;

namespace Snowflake.Data.Tests.UnitTests.Telemetry;


public sealed class SnowflakeTelemetryModuleTest
{
    private Mock<IMockRestRequester> _mockRestRequester;
    private ActivityListener _listener;

    [SetUp]
    public void SetUp()
    {
        _mockRestRequester = new Mock<IMockRestRequester>();
        _mockRestRequester.Setup(r => r.Post<NullDataResponse>(It.IsAny<IRestRequest>()))
            .Returns(new NullDataResponse { success = true });
        _mockRestRequester.Setup(r => r.PostAsync<NullDataResponse>(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NullDataResponse { success = true });

        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivityStarter.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    [TearDown]
    public void TearDown()
    {
        _listener?.Dispose();
    }

    [SFFact]
    public void TestRegisterCreatesSessionModule()
    {
        // Arrange
        var sessionId = "register-test-1";
        var session = CreateSession(ref sessionId);

        // Act & Assert - should not throw
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Register(session));
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule);
        Assert.Equal(session.sessionToken, sessionModule.SessionToken);

        // Cleanup
        SnowflakeTelemetryModule.Unregister(sessionId);
    }

    [SFFact]
    public void TestRegisterSameSessionUpdatesToken()
    {
        // Arrange
        var sessionId = "register-test-2";
        var session = CreateSession(ref sessionId);
        SnowflakeTelemetryModule.Register(session);
        var staleSessionToken = session.sessionToken;

        // Update token on session
        session.sessionToken = $"new-token-456-{Guid.NewGuid()}";
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule);
        Assert.Equal(staleSessionToken, sessionModule.SessionToken);

        // Act - second register should update token, not create new module
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Register(session));
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule2);
        Assert.Equal(sessionModule, sessionModule2);

        // Cleanup
        SnowflakeTelemetryModule.Unregister(sessionId);
    }

    [SFFact]
    public void TestUnregisterNonExistentSessionDoesNotThrow()
    {
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Unregister("non-existent-session"));
    }

    [SFFact]
    public void TestUnregisterDisposesModule()
    {
        // Arrange
        var sessionId = "unregister-test-1";
        var session = CreateSession(ref sessionId);
        SnowflakeTelemetryModule.Register(session);
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule);
        Assert.False(sessionModule.IsDisposed);

        // Act & Assert - should not throw
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Unregister(sessionId));
        Assert.False(SnowflakeTelemetryModule.TryGetSession(session.sessionId, out _));
        Assert.True(sessionModule.IsDisposed);

        // Unregistering again should be a no-op
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Unregister(sessionId));
        Assert.False(SnowflakeTelemetryModule.TryGetSession(session.sessionId, out _));
        Assert.True(sessionModule.IsDisposed);
    }

    [SFFact]
    public async Task TestUnregisterAsyncFlushesAndDisposes()
    {
        // Arrange
        var sessionId = "unregister-async-test-1";
        var session = CreateSession(ref sessionId);
        SnowflakeTelemetryModule.Register(session);
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule);
        var activity = new Activity("whatever");
        activity.Start();
        sessionModule.OnActivityStopped(activity);

        // Act
        Assert.Equal(1, sessionModule.CurrentBufferSize);
        await SnowflakeTelemetryModule.UnregisterAsync(sessionId, CancellationToken.None);
        Assert.True(sessionModule.IsDisposed);
        Assert.Equal(0, sessionModule.CurrentBufferSize);

        // Assert - calling again should be no-op
        await SnowflakeTelemetryModule.UnregisterAsync(sessionId, CancellationToken.None);
    }

    [SFFact]
    public async Task TestUnregisterAsyncForNonExistentSessionDoesNotThrow()
    {
        await SnowflakeTelemetryModule.UnregisterAsync("non-existent-async", CancellationToken.None);
    }

    [InlineData(true)]
    [InlineData(false)]
    public void TestOnActivityStoppedRoutesToCorrectSession(bool useCustomClientTelemetry)
    {
        // Arrange
        var sessionId = "routing-test-1";
        var sessionId2 = "routing-test-2";
        var session = CreateSession(ref sessionId);
        var session2 = CreateSession(ref sessionId2);
        SnowflakeTelemetryModule.Register(session);
        SnowflakeTelemetryModule.Register(session2);
        SnowflakeTelemetryModule.TryGetSession(session.sessionId, out var sessionModule);
        SnowflakeTelemetryModule.TryGetSession(session2.sessionId, out var sessionModule2);

        // Act - create and stop an activity with the sessionId
        var activity = useCustomClientTelemetry
            ? CreateCommand(session).StartActivity("TestRoutingCustom")
            : session.StartActivity("TestRouting");

        Assert.NotNull(activity);
        activity.SetSuccess();

        if (!SpinWait.SpinUntil(() => sessionModule.CurrentBufferSize == 1, TimeSpan.FromMinutes(1)))
            Assert.Fail("Expected to observe activity!");

        Assert.Equal(0, sessionModule2.CurrentBufferSize);

        // Cleanup
        SnowflakeTelemetryModule.Unregister(sessionId);
        SnowflakeTelemetryModule.Unregister(sessionId2);
    }

    [SFFact]
    public void TestRegisterWithTelemetryDisabledDoesNotCreateModule()
    {
        // Arrange - session with telemetry disabled
        var sessionId = "disabled-test-1";
        var session = CreateSession(ref sessionId, telemetryEnabled: false);

        // Act - Register should return early without creating a module
        SnowflakeTelemetryModule.Register(session);

        // Assert - Unregister is a no-op (no module was created),
        // and creating an activity for this session doesn't crash
        var source = new ActivitySource(ActivityStarter.ActivitySourceName);
        using var activity = source.StartActivity("ShouldNotRoute", ActivityKind.Client);
        activity?.SetTag(TelemetryTags.SessionId, sessionId);
        activity?.Stop();

        // Cleanup - should be a no-op since nothing was registered
        Assert.DoesNotThrow(() => SnowflakeTelemetryModule.Unregister(sessionId));
    }

    private SnowflakeDbCommand CreateCommand(SFSession session)
    {
        var snowflakeDbConnection = new SnowflakeDbConnection("someConnectionString")
        {
            SfSession = session,
            _connectionState = ConnectionState.Open
        };
        return new SnowflakeDbCommand(snowflakeDbConnection);
    }

    private SFSession CreateSession(ref string sessionId, bool telemetryEnabled = true)
    {
        var connectionStr = new StringBuilder()
            .Append("authenticator=snowflake;account=some_account;user=some_user;")
            .Append("password=fake_pwd;")
            .Append("db=testDb;role=WHATEVER_ROLE;warehouse=testWarehouse;host=localhost;port=443;scheme=https")
            .ToString();

        var distinctSuffix = Guid.NewGuid().ToString("N");
        sessionId += distinctSuffix;

        var session = new Mock<SFSession>(connectionStr, new SessionPropertiesContext(), _mockRestRequester.Object)
        {
            Object =
            {
                sessionId = sessionId,
                warehouse = "WH",
                role = "ROLE",
                database = "DB",
                sessionToken = $"token-{sessionId}"
            }
        };
        session.Setup(x => x.IsClientTelemetryEnabled()).Returns(telemetryEnabled);

        return session.Object;
    }
}
