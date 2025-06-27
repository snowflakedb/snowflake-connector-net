## Using Connection Pools

### Multiple Connection Pools

Snowflake .NET Driver v4.0.0 provides multiple pools with couple of additional features in comparison to the previous implementation.

Each pool is identified by the <b>entire connection string</b>. Order of connection string parameters is significant and the same connection parameters
ordered differently, lead to two different pools being used.
  - Please also note that this implies that if a connection string is changed (e.g. to apply a different `QUERY_TAG`), it will belong to a different Pool.
  - Specifically for `QUERY_TAG`, if you wish to apply a different `QUERY_TAG` yet still make sure the connection belongs to the same Pool as the others, consider not applying it on the Connection string, but instead using [SnowflakeDbCommand.QueryTag](https://github.com/snowflakedb/snowflake-connector-net/blob/v4.1.0/Snowflake.Data.Tests/IntegrationTests/SFDbCommandIT.cs#L1636)

All the pool parameters can be controlled from the connection string.

Pool interface is also maintained by the [SnowflakeDbConnectionPool.cs](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs).
However, some operations (eg. setting pool parameters from this SnowflakeDbConnectionPool class) are not possible having in mind multiple pools and possibly their different setup.
For that a [SnowflakeDbSessionPool.cs](/Snowflake.Data/Client/SnowflakeDbSessionPool.cs) is provided by
- [SnowflakeDbSessionPool.GetPool(connectionString)](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs#L45)
- [SnowflakeDbSessionPool.GetPool(connectionString, securePassword)](https://github.com/snowflakedb/snowflake-connector-net/blob/master/Snowflake.Data/Client/SnowflakeDbConnectionPool.cs#L51).
to control pool settings from the code. Changed pool settings are not reflected by their connection string therefore recommended way is to control the pool from the connection string.

### Pool Lifecycle

Single pool is instantiated each time an application creates and opens a connection for the first time using particular connection string.
Pool can be also initialized when accessing for the first time from <b>SnowflakeDbConnectionPool.GetPool</b>.

From that moment the pool tracks and maintains connections matching exactly this connection string.
Pool is responsible for destroying and recreating connections which are old enough (see [Expiration Timeout](#expiration-timeout)).
Number of connections is maintained within [Minimum pool size](#minimum-pool-size) and [Maximum pool size](#maximum-pool-size).
Connections in all their statuses are tracked:
- opening phase
- busy phase
- closed and returned to the pool (idle)
User can clean up the pool using methods: [Clear Pool](#clear-pool).

### Connection Lifecycle

#### Opening

When an application request to open a connection from the pool there are couple of possibilities:

1) Pool has idle connections already opened and they are provided immediately to the application
2) Pool has no idle connections but [Maximum pool size](#maximum-pool-size) is not reached in which case pool will open connection.
The slot for the new connection is reserved in the pool from the very beginning.
Even though opening a connection may take a while other threads are not blocked from accessing the pool.
3) When [Maximum pool size](#maximum-pool-size) is reached connection is waiting to be opened within period of
time controlled with [Pool Size Exceeded Timeout](#pool-size-exceeded-timeout).
When the timeout is exceeded then an exception will be thrown.

#### Busy

`Busy` connection is provided by the pool and it is counted to the pool size. It is returned to be reused during Close operation.
When application does not close connections it may hit the limit of [Maximum pool size](#maximum-pool-size).

#### Closing

When application closes the connection couple of things happen:
- Pending transactions will be rolled back (if any)
- Connection can be pooled when its properties are not changed
- Connection with changed: database, schema, warehouse or role can be:
  - pooled when OriginalPool mode enabled, see more: [Changed Session Behavior](#changed-session-behavior)
  - destroyed when Destroy mode is set

#### Evicting Connection

In order to prevent connection pooling the easiest way is to disable pooling. More on this here: [Pooling Enabled](#pooling-enabled).

However, in special cases an application may need to mark a single, opened connection to evict without turning off the pool.
When such a connection is closed it will not be pooled. Pool will create a new connection to maintain [Minimum pool size](#minimum-pool-size) if needed.

```cs
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
| [Pooling Enabled](#connection-timeout)                    | PoolingEnabled               | true    |                                 | Pooling connections authenticated with External Browser, OAuth Authorization Code Flow or Key-Pair Authentication without password is disabled by default |
| [Connection Timeout](#pooling-enabled)                    |                              | 300s    |                                 |                                                                                                                                                           |
| [Current Pool Size](#current-pool-size)                   |                              |         | GetCurrentPoolSize()            |                                                                                                                                                           |
| [Clear Pool](#clear-pool)                                 |                              |         | ClearPool() or ClearAllPools()  |                                                                                                                                                           |

#### Multiple pools

When a first connection is opened, a connection pool is created based on an exact matching algorithm that associates the pool with the connection string of the connection. Each connection pool is associated with a distinct connection string. When a new connection is opened, if the connection string is not an exact match to an existing pool, a new pool is created.

Different pools can have separate settings from the above settings for instance: minimum pool size or changed session behavior.

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

Ensures minimum specified size of the connections in a pool. Additional connections are created in the background during connection opening request.
When connections are being closed Connection Timeout is analysed for all the connections in a pool and the expired ones are being closed.
After that some connections will get recreated to ensure minimum size of the pool.

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
When an Idle Session Timeout is reached and an idle session is not returned within that period an exception will get thrown.

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

When an application does a change to the connection using one of SQL commands, for instance:
* `use schema`, `create schema`
* `use database`, `create database`
* `use warehouse`, `create warehouse`
* `use role`, `create role`
* `drop`
then such an affected connection is marked internally as no longer matching with the pool it originated from (it becomes a "dirty" connection).
Keep in mind that create commands automatically set active the created object within current connection
(eg. [create database](https://docs.snowflake.com/en/sql-reference/sql/create-database#general-usage-notes)).

Pool has two different approaches to connections altered with above way:
* Destroy connection
* Pool it back to the Original Pool

1) Destroy Connection Mode

To enable this pool mode parameter ChangedSession should be set to `Destroy` or entirely skipped (Destroy is the default pool behavior).
In this mode application may safely alter connection properties: schema, database, warehouse or role. Such a dirty connection no longer matching
with the connection string will not get pooled any more. The pool marks it internally as `dirty` and ensures it gets removed
when no longer used (closed) by the application.

Since such connections do not return to the pool, it will recreate necessary number of connections to satisfy the Minimum Pool Size requirement.

```cs
var connectionString = ConnectionString + ";ChangedSession=Destroy";
var connection = new SnowflakeDbConnection(connectionString);

connection.Open();
var randomSchemaName = Guid.NewGuid();
connection.CreateCommand($"create schema \"{randomSchemaName}\").ExecuteNonQuery(); // schema gets changed
// application is running commands on a schema with random name
connection.Close(); // connection does not return to the original pool and gets destroyed; pool will reconstruct the pool
                    // with new connections accordingly to the MinPoolSize

var connection2 = new SnowflakeDbConnection(connectionString);
connection2.Open();
// operations here will be performed against schema indicated in the ConnectionString
```

2) Pooling Changed Session to the Original Pool

When parameter ChangedSession is set to `OriginalPool` it allows the connection to be pooled back to the original pool from which it came from.

<u>Disclaimer for OriginalPool Mode</u>

When application reuses connections affected by the above commands (use/create) it might get to a point when using a connection
provided by the pool it gets SQL syntax errors since tables, procedures, stages and other database objects do not exists because the operations
are executed using changed database, schema, user or role no longer matching connection string.
Reusing connection from a pool requires attention from the code perspective and ensuring that each retrieved connection uses appropriate database, schema, warehouse or role.
This mode is purely for backward compatibility but is not recommended to be used. It is also not a default.

```cs
var connectionString = ConnectionString + ";ChangedSession=OriginalPool;MinPoolSize=1;MaxPoolSize=1";
var connection = new SnowflakeDbConnection(connectionString);

connection.Open();
var randomSchemaName = Guid.NewGuid();
connection.CreateCommand($"create schema \"{randomSchemaName}\").ExecuteNonQuery(); // schema gets changed
// application is running commands on a schema with random name
connection.Close(); // connection returns to the original pool but it's schema will no longer match with initial value

var connection2 = new SnowflakeDbConnection(connectionString);
connection2.Open();
// operations here will be performed against schema: randomSchemaName
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

Enables or disables connection pooling for the pool identified by a given connection string.

<u>For security reasons pooling is disabled by default for:
- External Browser
- OAuth Authorization Code Flow
- Key-Pair Authentication (unless password for key is provided).</u>

It can be enabled with a connection string parameter if needed.
However, be warned that using:
- token key file accessible by others and used to authorize connection
- shared environment with an external browser or OAuth Authorization Code Flow authenticated connections
leads to vulnerabilities and is not recommended.

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

Allows to check size of the given pool programatically. It is total number of all the connections: idle, busy and during initialization.

```cs
var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
var poolSize = pool.GetCurrentPoolSize();
// default pool size is 2
Assert.AreEqual(2, poolSize);
```

At the SnowflakeDbConnectionPool there is also a way to get sum of connections from all the pools.

```cs
var pool1 = SnowflakeDbConnectionPool.GetPool(connectionString + ";MinPoolSize=2");
var pool2 = SnowflakeDbConnectionPool.GetPool(connectionString + ";MinPoolSize=3");
var poolsSize = SnowflakeDbConnectionPool.GetCurrentPoolSize();
Assert.AreEqual(5, poolSize);
```

#### Clear Pool

Interface allows to clear a particular pool or all the pools initiated by an application.
Please keep in mind that a default of min pool size will be maintained.

```cs
var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
```

There is also a way to clear all the pools initiated by an application.

```cs
SnowflakeDbConnectionPool.ClearAllPools();
```
