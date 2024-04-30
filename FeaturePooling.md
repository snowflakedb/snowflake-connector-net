## Using Connection Pools

### Multiple Connection Pools

v4.0.0 version of Snowflake .NET Driver provides multiple pools with couple of additional features according to the previous implementation.

| Connection Pool Feature    | Connection String Parameter  | Default      | Method               | Description |
|----------------------------|------------------------------|--------------|----------------------|-------------|
| Multiple pools             |                              |              |                      | TODO        |
| Minimum pool size          | MinPoolSize                  | 2            |                      |             |
| Maximum pool size          | MaxPoolSize                  | 10           |                      |             |
| Changed Session Behavior   | ChangedSession               | OriginalPool |                      |             |
| Pool Size Exceeded Timeout | WaitingForIdleSessionTimeout | 30s          |                      |             |
| Expiration Timeout         | ExpirationTimeout            | 60m          |                      |             |
| Pooling Enabled            | PoolingEnabled               | true         |                      |             |
| Connection Timeout         |                              | 300s         |                      |             |
| Current Pool Size          |                              |              | GetCurrentPoolSize() |             |
| Clear Pool                 |                              |              | ClearPool()          |             |

#### Multiple pools

When a connection is first opened, a connection pool is created based on an exact matching algorithm that associates the pool with the connection string in the connection. Each connection pool is associated with a distinct connection string. When a new connection is opened, if the connection string is not an exact match to an existing pool, a new pool is created.

```cs
using (var connection = new SnowflakeDbConnection(ConnectionString + ";application=App1"))
{
    connection.Open();
    // Pool 1 is created
}

using (var connection = new SnowflakeDbConnection(ConnectionString + ";application=App2"))
{
    connection.Open();
    // Pool 2 is created
}
```

#### Minimum pool size

Ensures minimal specified size of the connections in a pool. Additional connections are created in the background during connection opening request.
When connections are being closed Connection Timeout is analysed for all the connections in a pool and the expired ones are being closed.
After that some connections will get recreated to ensure minimal size of the pool.

```cs
var connectionString = ConnectionString + ";MinPoolSize=10";
using (var connection = new SnowflakeDbConnection(connectionString))
{
    connection.Open();
    // Pool of size 10 is created
}
var poolSize = SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize();
Assert.AreEqual(10, poolSize);
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
var connectionString = ConnectionString + ";MaxPoolSize=2";

Task[] tasks = new Task[8];
for (int i = 0; i < tasks.Length; i++)
{
    var taskName = $"Task {i}";
    tasks[i] = Task.Run(() =>
    {
        using (var connection = new SnowflakeDbConnection(connectionString))
        {
            StopWatch sw = new StopWatch();

            // register opening time
            sw.Start();
            connection.Open();
            sw.Stop();

            // output
            Console.WriteLine($"{taskName} waited {Math.Round((double)sw.ElapsedMilliseconds / 1000)} seconds");

            // wait 2s before closing the connection
            Thread.Sleep(2000);
        }
    });
}
Task.WaitAll(tasks);

// check current pool size
var poolSize = SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize();
Assert.AreEqual(2, poolSize);

// output:
// Task 1 waited 0 seconds
// Task 4 waited 0 seconds
// Task 7 waited 2 seconds
// Task 0 waited 2 seconds
// Task 6 waited 4 seconds
// Task 3 waited 4 seconds
// Task 2 waited 6 seconds
// Task 5 waited 6 seconds
```

#### Changed Session Behavior

When a connection gets changed...

```cs
```

#### Pool Size Exceeded Timeout

The timeout for providing a connection when Max Pool Size is reached.
* When timeout to provide new connection is exceeded and there are no idle connections in the pool an exception will be thrown
* When specified as 0, an exception will be thrown immediately if there are no idle connections in the pool

```cs
var connectionString = ConnectionString + ";MaxPoolSize=2;WaitingForIdleSessionTimeout=3";

Task[] tasks = new Task[8];
for (int i = 0; i < tasks.Length; i++)
{
    var taskName = $"Task {i}";
    tasks[i] = Task.Run(() =>
    {
        try
        {
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                StopWatch sw = new StopWatch();

                // register opening time
                sw.Start();
                connection.Open();
                sw.Stop();

                // output
                Console.WriteLine($"{taskName} waited {Math.Round((double)sw.ElapsedMilliseconds / 1000)} seconds");

                // wait 2s before closing the connection
                Thread.Sleep(2000);
            }
        }
        catch (SnowflakeDbException ex)
        {
            Console.WriteLine($"{taskName} - {ex.Message}");
        }
    });
}
Task.WaitAll(tasks);

// check current pool size
var poolSize = SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize();
Assert.AreEqual(2, poolSize);

// output:
// Task 3 waited 0 seconds
// Task 0 waited 0 seconds
// Task 5 waited 2 seconds
// Task 6 waited 2 seconds
// Task 4 - Error: Snowflake Internal Error: Unable to connect. Could not obtain a connection from the pool within a given timeout SqlState: 08006, VendorCode: 270001, QueryId:
// Task 7 - Error: Snowflake Internal Error: Unable to connect. Could not obtain a connection from the pool within a given timeout SqlState: 08006, VendorCode: 270001, QueryId:
// Task 1 - Error: Snowflake Internal Error: Unable to connect. Could not obtain a connection from the pool within a given timeout SqlState: 08006, VendorCode: 270001, QueryId:
// Task 2 - Error: Snowflake Internal Error: Unable to connect. Could not obtain a connection from the pool within a given timeout SqlState: 08006, VendorCode: 270001, QueryId:
```

#### Expiration Timeout

Overall timeout for entire connection lifetime
* When reached connection is always removed
* After pruning, Min Pool Size is checked to achieve expected number of connections in the pool

```cs
var connectionString = ConnectionString + ";MinPoolSize=1;ExpirationTimeout=2";
var connection1 = new SnowflakeDbConnection(connectionString);
var connection2 = new SnowflakeDbConnection(connectionString);
var connection3 = new SnowflakeDbConnection(connectionString);

connection1.Open();
connection2.Open();
connection1.Close();
connection2.Close();

// 2 connections are in the pool
Assert.AreEqual(2, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

Thread.Sleep(2000);

connection3.Open();
connection3.Close();

// both previous connections have expired
Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
```

#### Connection Timeout

Total timeout in seconds when connecting to Snowflake.
Equivalent of https://learn.microsoft.com/en-us/dotnet/api/system.data.idbconnection.connectiontimeout?view=net-6.0

```cs
var connectionString = ConnectionString + ";connection_timeout=160";
using (var connection = new SnowflakeDbConnection(connectionString))
{
    connection.Open();
}
```

#### Pooling Enabled

Enables or disables connection pooling for the given connection string.

```cs
var connectionString = ConnectionString + ";PoolingEnabled=false";
using (var connection = new SnowflakeDbConnection(connectionString))
{
    connection.Open();
}

// no connection in the pool
var poolSize = SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize();
Assert.AreEqual(0, poolSize);
```

#### Current Pool Size

```cs
var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
var poolSize = pool.GetCurrentPoolSize();
// default pool size is 2
Assert.AreEqual(2, poolSize);
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
