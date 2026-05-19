using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.IntegrationTests;

internal readonly record struct AmbientContext(string Name, IConnectionManager ConnectionManager, IConnectionManagerFactory ConnectionManagerFactory);

internal class ConnectionManagerTestsFacade : IConnectionManager
{
    private readonly IConnectionManager _globalInstance;
    private static readonly AsyncLocal<AmbientContext> t_asyncLocalConnectionManager = new();
    private static volatile int s_initState = 0;

    private IConnectionManager ConnectionManagerImplementation => t_asyncLocalConnectionManager.Value.ConnectionManager ?? _globalInstance;

    public static void Init()
    {
        Interlocked.Increment(ref s_initState);
        var manager = ConnectionManagerFactory.Singleton.CreateConnectionManager(SnowflakeDbConnectionPool.DefaultConnectionPoolType);
        var facade = new ConnectionManagerTestsFacade(manager);
        SnowflakeDbConnectionPool.ReplaceConnectionManager(facade);
    }

    public static void RegisterDedicatedContext(string ambientContextName, ConnectionPoolType connectionPoolType, IConnectionManagerFactory connectionManagerFactory = null)
    {
        if (s_initState == 0)
            throw new SFConnectionManagerTestsException("This is unsupported. You need to initialize this component before you can register dedicated context!");

        connectionManagerFactory ??= ConnectionManagerFactory.Singleton;
        var connectionManager = connectionManagerFactory.CreateConnectionManager(connectionPoolType);
        t_asyncLocalConnectionManager.Value = new AmbientContext
        {
            Name = ambientContextName,
            ConnectionManager = connectionManager,
            ConnectionManagerFactory = connectionManagerFactory
        };
    }

    public static void RegisterDedicatedContext(SFBaseTestAsync test, ConnectionPoolType connectionPoolType) =>
        RegisterDedicatedContext(test.GetType().Name, connectionPoolType);

    public static void UnregisterDedicatedContext(SFBaseTestAsync test) => UnregisterDedicatedContext(test.GetType().Name);

    public static void UnregisterDedicatedContext(string ambientContextName)
    {
        if (t_asyncLocalConnectionManager.Value.Name != ambientContextName)
            throw new SFConnectionManagerTestsException($"The connection manager is not initialized. Call {nameof(RegisterDedicatedContext)} in your test ctr.");

        t_asyncLocalConnectionManager.Value = default;
    }

    private ConnectionManagerTestsFacade(IConnectionManager connectionManagerImplementation)
    {
        _globalInstance = connectionManagerImplementation;
    }

    public SFSession GetSession(string connectionString, SessionPropertiesContext sessionContext) => ConnectionManagerImplementation.GetSession(connectionString, sessionContext);

    public Task<SFSession> GetSessionAsync(string connectionString, SessionPropertiesContext sessionContext, CancellationToken cancellationToken) => ConnectionManagerImplementation.GetSessionAsync(connectionString, sessionContext, cancellationToken);

    public bool AddSession(SFSession session) => ConnectionManagerImplementation.AddSession(session);

    public void ReleaseBusySession(SFSession session) => ConnectionManagerImplementation.ReleaseBusySession(session);

    public void ClearAllPools() => ConnectionManagerImplementation.ClearAllPools();

    public void SetMaxPoolSize(int maxPoolSize) => ConnectionManagerImplementation.SetMaxPoolSize(maxPoolSize);

    public int GetMaxPoolSize() => ConnectionManagerImplementation.GetMaxPoolSize();

    public void SetTimeout(long connectionTimeout) => ConnectionManagerImplementation.SetTimeout(connectionTimeout);

    public long GetTimeout() => ConnectionManagerImplementation.GetTimeout();

    public int GetCurrentPoolSize() => ConnectionManagerImplementation.GetCurrentPoolSize();

    public bool SetPooling(bool poolingEnabled) => ConnectionManagerImplementation.SetPooling(poolingEnabled);

    public bool GetPooling() => ConnectionManagerImplementation.GetPooling();

    public SessionPool GetPool(string connectionString) => ConnectionManagerImplementation.GetPool(connectionString);

    public SessionPool GetPool(string connectionString, SessionPropertiesContext sessionContext) => ConnectionManagerImplementation.GetPool(connectionString, sessionContext);

    public IConnectionManager Recycle(ConnectionPoolType requestedPoolType)
    {
        if (t_asyncLocalConnectionManager.Value.ConnectionManager == null)
            throw new SFConnectionManagerTestsException($"The connection manager is not initialized. If your test modifies global state, it needs to setup an ambient context first, to reduce interference with remaining tests. Call {nameof(RegisterDedicatedContext)} in your test ctr.");

        var configurationManager = t_asyncLocalConnectionManager.Value.ConnectionManagerFactory.CreateConnectionManager(requestedPoolType);
        var newAmbientContext = t_asyncLocalConnectionManager.Value with
        {
            ConnectionManager = configurationManager
        };
        t_asyncLocalConnectionManager.Value = newAmbientContext;

        return this;
    }

    public ConnectionPoolType Type => ConnectionManagerImplementation.Type;

    // TODO tests
    public sealed class SFConnectionManagerTestsException : Exception
    {
        public SFConnectionManagerTestsException(string message) : base(message) { }
    }
}
