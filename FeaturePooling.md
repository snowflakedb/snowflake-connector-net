## Using Connection Pools

### Multiple Connection Pools

v4.0.0 version of Snowflake .NET Driver provides multiple pools with couple of additional features according to the previous implementation.

| Connection Pool Feature  | Connection String Parameter  | Default      | Method               | Description |
|--------------------------|------------------------------|--------------|----------------------|-------------|
| Multiple pools           |                              |              |                      | TODO        |
| Minimum pool size        | MinPoolSize                  | 2            |                      |             |
| Maximum pool size        | MaxPoolSize                  | 10           |                      |             |
| Changed Session Behavior | ChangedSession               | OriginalPool |                      |             |
| Idle Session Timeout     | WaitingForIdleSessionTimeout | 30s          |                      |             |
| Expiration Timeout       | ExpirationTimeout            | 60m          |                      |             |
| Pooling Enabled          | PoolingEnabled               | true         |                      |             |
| Connection Timeout       |                              | 300s         |                      |             |
| Current Pool Size        |                              |              | GetCurrentPoolSize() |             |
| Clear Pool               |                              |              | ClearPool()          |             |

#### Multiple pools

```cs
```

#### Minimum pool size

Ensures minimal specified size of the connections in a pool. Additional connections are created in the background during connection opening request.
When connections are being closed Connection Timeout is analysed for all the connections in a pool and the expired ones are being closed.
After that some connections will get recreated to ensure minimal size of the pool.

```cs
```

#### Maximum pool size

Latest pool version ensures maximum size of the pool.
What counts for that are:
- idle connections
- busy connections (provided by the pool to the application)
- connections during opening phase
When a maximum pool size is reached any request to provide (open) another connection is waiting for any idle session to be returned to the pool.
When an Idle Session Timeout is reached and an idle session is not returned an exception will get thrown.

```cs
```

#### Changed Session Behavior

When a connection gets changed...

```cs
```

#### Idle Session Timeout

```cs
```

#### Expiration Timeout

```cs
```

#### Connection Timeout

```cs
```

#### Pooling Enabled

```cs
```

#### Current Pool Size

```cs
```

#### Clear Pool

```cs
```

### Single Connection Pool

DEPRECATED VERSION

Instead of creating a connection each time your client application needs to access Snowflake, you can define a cache of Snowflake connections that can be reused as needed.
Connection pooling usually reduces the lag time to make a connection. However, it can slow down client failover to an alternative DNS when a DNS problem occurs.

The Snowflake .NET driver provides the following functions for managing connection pools.

| Function                                        | Description                                                                                              |
|-------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| SnowflakeDbConnectionPool.ClearAllPools()       | Removes all connections from the connection pool.                                                        |
| SnowflakeDbConnection.SetMaxPoolSize(n)         | Sets the maximum number of connections for the connection pool, where _n_ is the number of connections.  |
| SnowflakeDBConnection.SetTimeout(n)             | Sets the number of seconds to keep an unresponsive connection in the connection pool.                    |
| SnowflakeDbConnectionPool.GetCurrentPoolSize()  | Returns the number of connections currently in the connection pool.                                      |
| SnowflakeDbConnectionPool.SetPooling()          | Determines whether to enable (`true`) or disable (`false`) connecing pooling. Default: `true`.           |

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
