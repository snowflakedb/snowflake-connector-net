## Using Connection Pools

### Multiple Connection Pools

v4.0.0 version of Snowflake .NET Driver provides multiple pools with couple of additional features according to the previous implementation.

| Connection Pool Feature    | Connection String Parameter  | Default      | Method                         |
|----------------------------|------------------------------|--------------|--------------------------------|
| Multiple pools             |                              |              |                                |
| Minimum pool size          | MinPoolSize                  | 2            |                                |
| Maximum pool size          | MaxPoolSize                  | 10           |                                |
| Changed Session Behavior   | ChangedSession               | OriginalPool |                                |
| Pool Size Exceeded Timeout | WaitingForIdleSessionTimeout | 30s          |                                |
| Expiration Timeout         | ExpirationTimeout            | 60m          |                                |
| Pooling Enabled            | PoolingEnabled               | true         |                                |
| Connection Timeout         |                              | 300s         |                                |
| Current Pool Size          |                              |              | GetCurrentPoolSize()           |
| Clear Pool                 |                              |              | ClearPool() or ClearAllPools() |

#### Multiple pools

When a first connection is opened, a connection pool is created based on an exact matching algorithm that associates the pool with the connection string of the connection. Each connection pool is associated with a distinct connection string. When a new connection is opened, if the connection string is not an exact match to an existing pool, a new pool is created.

Different pools can have separate settings from the above settings for instance: minimal pool size or changed session behavior.

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

When an application does a change to the connection using one of SQL commands:
* `use schema`, `create schema`
* `use database`, `create database`
* `use warehouse`, `create warehouse`
* `use role`, `create role`

then such an affected connection is marked internally as no longer matching with the pool it originated from.
When parameter ChangedSession is set to `OriginalPool` it allows the connection to be pooled.
Parameter ChangedSession set to `Destroy` (default) ensures that the connection is not pooled and after Close is called the connection will be removed.
The pool will recreate necessary connections according to the minimal pool size.

1) ChangedSession = Destroy

In this mode application may safely alter session properties: schema, database, warehouse, role. Connection not matching
with the connection string will not get pooled.

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

2) ChangedSession = OriginalPool

When application reuses connections affected by the above commands it might get to a point when using a connection
it gets errors since tables, procedures, stages do not exists cause the operations are executed using wrong
database, schema, user or role. This mode is purely for backward compatibility but is not recommended to be used.

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
