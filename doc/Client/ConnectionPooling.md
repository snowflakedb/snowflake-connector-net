## Using Connection Pools

### Multiple Connection Pools

Starting with v4.0.0, the Snowflake .NET Driver provides multiple connection pools with additional features over the previous single-pool implementation.

Each pool is identified by the exact **connection string**. The order of parameters is significant — the same parameters in a different order create separate pools.

All pool parameters can be set in the connection string.

The pool interface is exposed through [SnowflakeDbConnectionPool.cs](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs).
Some operations (e.g. setting pool parameters programmatically) require access to a specific pool via [SnowflakeDbSessionPool.cs](/Snowflake.Data/Client/SnowflakeDbSessionPool.cs), obtained through:
- [SnowflakeDbSessionPool.GetPool(connectionString)](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs#L45)
- [SnowflakeDbSessionPool.GetPool(connectionString, securePassword)](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs#L51)

Pool settings modified programmatically are not reflected in the connection string. The recommended approach is to control pool behavior through the connection string.

### Pool Lifecycle

A pool is created the first time a connection is opened with a particular connection string, or when accessed via `SnowflakeDbConnectionPool.GetPool`.

From that point, the pool tracks and maintains all connections matching that connection string.
The pool handles destroying and recreating expired connections (see [Expiration Timeout](#expiration-timeout)) and maintains the connection count within [Minimum Pool Size](#minimum-pool-size) and [Maximum Pool Size](#maximum-pool-size).

Use [Clear Pool](#clear-pool) to remove connections from a pool.

### Connection Lifecycle

#### Opening

When the application opens a connection, the pool follows this logic:

1. If idle connections are available, one is returned immediately.
2. If no idle connections exist but [Maximum Pool Size](#maximum-pool-size) has not been reached, a new connection is opened. The slot is reserved immediately; other threads are not blocked while the connection is being established.
3. If [Maximum Pool Size](#maximum-pool-size) has been reached, the request waits for an idle connection within the [Pool Size Exceeded Timeout](#pool-size-exceeded-timeout). If the timeout is exceeded, an exception is thrown.

#### Busy

A busy connection has been provided by the pool and counts toward the pool size. It is returned to the pool when the application closes it. Failure to close connections may exhaust the [Maximum Pool Size](#maximum-pool-size).

#### Closing

When a connection is closed:
- Pending transactions are rolled back
- If connection properties (database, schema, warehouse, role) have not changed, the connection is returned to the pool
- If properties have changed, behavior depends on [Changed Session Behavior](#changed-session-behavior):
  - `OriginalPool`: the connection is returned to its original pool
  - `Destroy`: the connection is destroyed

#### Evicting a Connection

To prevent a specific connection from being pooled without disabling pooling entirely, call `PreventPooling()` on an open connection. When that connection is closed, it is destroyed instead of being returned to the pool. The pool creates a new connection if needed to maintain [Minimum Pool Size](#minimum-pool-size).

```csharp
using (var connection = new SnowflakeDbConnection(ConnectionString))
{
    connection.Open();
    connection.PreventPooling();
}
```

### Pool Interfaces

| Connection Pool Feature                                   | Connection String Parameter  | Default | Method                          | Info                                                                                                                                                      |
|-----------------------------------------------------------|------------------------------|---------|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Multiple pools](#multiple-pools)                         |                              |         |                                 |                                                                                                                                                           |
| [Minimum pool size](#minimum-pool-size)                   | MinPoolSize                  | 2       |                                 |                                                                                                                                                           |
| [Maximum pool size](#maximum-pool-size)                   | MaxPoolSize                  | 10      |                                 |                                                                                                                                                           |
| [Changed Session Behavior](#changed-session-behavior)     | ChangedSession               | Destroy |                                 | Destroy or OriginalPool                                                                                                                                   |
| [Pool Size Exceeded Timeout](#pool-size-exceeded-timeout) | WaitingForIdleSessionTimeout | 30s     |                                 | Values can be provided with postfix [ms], [s], [m]                                                                                                        |
| [Expiration Timeout](#expiration-timeout)                 | ExpirationTimeout            | 60m     |                                 |                                                                                                                                                           |
| [Pooling Enabled](#pooling-enabled)                       | PoolingEnabled               | true    |                                 | Pooling connections authenticated with External Browser, OAuth Authorization Code Flow or Key-Pair Authentication without password is disabled by default |
| [Connection Timeout](#connection-timeout)                 |                              | 300s    |                                 |                                                                                                                                                           |
| [Current Pool Size](#current-pool-size)                   |                              |         | GetCurrentPoolSize()            |                                                                                                                                                           |
| [Clear Pool](#clear-pool)                                 |                              |         | ClearPool() or ClearAllPools()  |                                                                                                                                                           |

#### Multiple pools

A new connection pool is created whenever a connection string does not exactly match an existing pool. Different pools can have independent settings for minimum pool size, changed session behavior, etc.

```csharp
using var connection = new SnowflakeDbConnection($"{ConnectionString};application=App1")
await connection.OpenAsync(cancellationToken); // Pool 1 is created
    

using var connection = new SnowflakeDbConnection($"{ConnectionString};application=App2")
await connection.OpenAsync(cancellationToken); // Pool 2 is created

```

#### Minimum pool size

Ensures the pool maintains at least the specified number of connections. Additional connections are created in the background when a connection is opened. During connection close, expired connections are removed and then new ones are created to restore the minimum size.

```csharp
using var connection = new SnowflakeDbConnection($"{ConnectionString};application=App1") // Pool of size 10 is created
var poolSize = SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize();
Assert.AreEqual(10, poolSize);
```

#### Maximum pool size

Limits the total number of connections (idle, busy, and opening) in the pool.

When the maximum is reached, new connection requests wait for an idle session to become available. If no session is returned within the [Pool Size Exceeded Timeout](#pool-size-exceeded-timeout), an exception is thrown.

```csharp
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

When an application alters a connection's session using SQL commands such as:
* `USE SCHEMA`, `CREATE SCHEMA`
* `USE DATABASE`, `CREATE DATABASE`
* `USE WAREHOUSE`, `CREATE WAREHOUSE`
* `USE ROLE`, `CREATE ROLE`
* `DROP`

the connection is marked as no longer matching its original pool. Note that `CREATE` commands automatically activate the created object within the current connection (e.g. [CREATE DATABASE](https://docs.snowflake.com/en/sql-reference/sql/create-database#general-usage-notes)).

Two modes are available:

**1) Destroy (default)**

Set `ChangedSession=Destroy` or omit the parameter. Altered connections are not returned to the pool — they are destroyed when closed. The pool creates new connections as needed to satisfy [Minimum Pool Size](#minimum-pool-size).

```csharp
var connectionString = ConnectionString + ";ChangedSession=Destroy";
var connection = new SnowflakeDbConnection(connectionString);

connection.Open();
var randomSchemaName = Guid.NewGuid();
connection.CreateCommand($"create schema \"{randomSchemaName}\"").ExecuteNonQuery(); // schema is changed
// application operates on the new schema
connection.Close(); // connection is destroyed; pool replenishes to MinPoolSize

var connection2 = new SnowflakeDbConnection(connectionString);
connection2.Open();
// operations use the schema from ConnectionString
```

**2) OriginalPool**

Set `ChangedSession=OriginalPool` to return altered connections to their original pool.

> **Warning**: Connections retrieved from the pool may have a different database, schema, warehouse, or role than the connection string specifies. This can lead to SQL errors if the application assumes the original context. This mode exists for backward compatibility and is not recommended.

```csharp
var connectionString = ConnectionString + ";ChangedSession=OriginalPool;MinPoolSize=1;MaxPoolSize=1";
var connection = new SnowflakeDbConnection(connectionString);

connection.Open();
var randomSchemaName = Guid.NewGuid();
connection.CreateCommand($"create schema \"{randomSchemaName}\"").ExecuteNonQuery(); // schema is changed
connection.Close(); // connection returns to the pool with the altered schema

var connection2 = new SnowflakeDbConnection(connectionString);
connection2.Open();
// operations execute against randomSchemaName, not the schema in ConnectionString
```

#### Pool Size Exceeded Timeout

The timeout for acquiring a connection when the pool is at maximum capacity.
* When the timeout expires with no idle connections available, an exception is thrown.
* A value of `0` causes immediate failure if no idle connections are available.

```csharp
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

Controls the maximum lifetime of a connection. Connections that exceed this timeout are removed from the pool. After removal, the pool creates new connections as needed to maintain [Minimum Pool Size](#minimum-pool-size).

```csharp
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

Total timeout in seconds when connecting to Snowflake. Equivalent to [IDbConnection.ConnectionTimeout](https://learn.microsoft.com/en-us/dotnet/api/system.data.idbconnection.connectiontimeout?view=net-6.0).

```csharp
var connectionString = ConnectionString + ";connection_timeout=160";
using (var connection = new SnowflakeDbConnection(connectionString))
{
    connection.Open();
}
```

#### Pooling Enabled

Enables or disables connection pooling for the pool identified by a given connection string.

For security reasons, pooling is disabled by default for:
- External Browser authentication
- OAuth Authorization Code Flow
- Key-Pair Authentication without a key password

Pooling can be explicitly enabled via the connection string if needed. However, using a token key file accessible to others or sharing an environment with browser/OAuth-authenticated connections introduces security risks and is not recommended.

```csharp
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

Returns the total number of connections in a pool (idle, busy, and initializing).

```csharp
var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
var poolSize = pool.GetCurrentPoolSize();
// default pool size is 2
Assert.AreEqual(2, poolSize);
```

To get the total connection count across all pools:

```csharp
var pool1 = SnowflakeDbConnectionPool.GetPool(connectionString + ";MinPoolSize=2");
var pool2 = SnowflakeDbConnectionPool.GetPool(connectionString + ";MinPoolSize=3");
var poolsSize = SnowflakeDbConnectionPool.GetCurrentPoolSize();
Assert.AreEqual(5, poolsSize);
```

#### Clear Pool

Removes connections from a specific pool or all pools. Note that [Minimum Pool Size](#minimum-pool-size) is maintained after clearing.

```csharp
var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
pool.ClearPool();
```

To clear all pools:

```csharp
SnowflakeDbConnectionPool.ClearAllPools();
```
