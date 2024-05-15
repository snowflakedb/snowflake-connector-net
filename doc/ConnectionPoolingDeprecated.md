## Using Connection Pools

### Single Connection Pool (DEPRECATED)

DEPRECATED VERSION

Instead of creating a connection each time your client application needs to access Snowflake, you can define a cache of Snowflake connections that can be reused as needed.
Connection pooling usually reduces the lag time to make a connection. However, it can slow down client failover to an alternative DNS when a DNS problem occurs.

The Snowflake .NET driver provides the following functions for managing connection pools.

| Function                                        | Description                                                                                             |
|-------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| SnowflakeDbConnectionPool.ClearAllPools()       | Removes all connections from the connection pool.                                                       |
| SnowflakeDbConnection.SetMaxPoolSize(n)         | Sets the maximum number of connections for the connection pool, where _n_ is the number of connections. |
| SnowflakeDBConnection.SetTimeout(n)             | Sets the number of seconds to keep an unresponsive connection in the connection pool.                   |
| SnowflakeDbConnectionPool.GetCurrentPoolSize()  | Returns the number of connections currently in the connection pool.                                     |
| SnowflakeDbConnectionPool.SetPooling()          | Determines whether to enable (`true`) or disable (`false`) connection pooling. Default: `true`.         |

The following sample demonstrates how to monitor the size of a connection pool as connections are added and dropped from the pool.

```cs
public void TestConnectionPoolClean()
{
  SnowflakeDbConnectionPool.ClearAllPools();
  SnowflakeDbConnectionPool.SetMaxPoolSize(2);
  var conn1 = new SnowflakeDbConnection();
  conn1.ConnectionString = ConnectionString;
  conn1.Open();
  Assert.AreEqual(ConnectionState.Open, conn1.State);

  var conn2 = new SnowflakeDbConnection();
  conn2.ConnectionString = ConnectionString + " retryCount=1";
  conn2.Open();
  Assert.AreEqual(ConnectionState.Open, conn2.State);
  Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  conn1.Close();
  conn2.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  var conn3 = new SnowflakeDbConnection();
  conn3.ConnectionString = ConnectionString + "  retryCount=2";
  conn3.Open();
  Assert.AreEqual(ConnectionState.Open, conn3.State);

  var conn4 = new SnowflakeDbConnection();
  conn4.ConnectionString = ConnectionString + "  retryCount=3";
  conn4.Open();
  Assert.AreEqual(ConnectionState.Open, conn4.State);

  conn3.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  conn4.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

  Assert.AreEqual(ConnectionState.Closed, conn1.State);
  Assert.AreEqual(ConnectionState.Closed, conn2.State);
  Assert.AreEqual(ConnectionState.Closed, conn3.State);
  Assert.AreEqual(ConnectionState.Closed, conn4.State);
}
```

<u>Note</u>
Some of the features and configurations available for [Multiple Connection Pools](ConnectionPooling.md) are not available for the old pool.
Following configurations/settings have no effect on [Single Connection Pool](ConnectionPoolingDeprecated.md):
- `poolingEnabled` setting, feature not configurable by connection string, instead you could use `SnowflakeDbConnectionPool.SetPooling(false)`
- `changedSession` setting, only `OriginalPool` behavior available
- `maxPoolSize` setting, feature not configurable by connection string, instead you could use `SnowflakeDbConnectionPool.SetMaxPoolSize()`
- `minPoolSize` setting, feature not available
- `waitingForIdleSessionTimeout` setting, feature not available
- `expirationTimeout` setting, feature not configurable by connection string, instead you could use `SnowflakeDbConnectionPool.SetTimeout()`.
