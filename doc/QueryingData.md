## Run a Query and Read Data

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "select * from t";
    IDataReader reader = cmd.ExecuteReader();

    while(reader.Read())
    {
        Console.WriteLine(reader.GetString(0));
    }

    conn.Close();
}
```

Note that for a `TIME` column, the reader returns a `System.DateTime` value. If you need a `System.TimeSpan` column, call the
`getTimeSpan` method in `SnowflakeDbDataReader`. This method was introduced in the v2.0.4 release.

Note that because this method is not available in the generic `IDataReader` interface, you must cast the object as
`SnowflakeDbDataReader` before calling the method. For example:

```cs
TimeSpan timeSpanTime = ((SnowflakeDbDataReader)reader).GetTimeSpan(13);
```

## Execute a query asynchronously on the server

You can run the query asynchronously on the server. The server responds immediately with `queryId` and continues to execute the query asynchronously.
Then you can use this `queryId` to check the query status or wait until the query is completed and get the results.
It is fine to start the query in one session and continue to query for the results in another one based on the queryId.

**Note**: There are 2 levels of asynchronous execution. One is asynchronous execution in terms of C# language (`async await`).
Another is asynchronous execution of the query by the server (you can recognize it by `InAsyncMode` containing method names, e. g. `ExecuteInAsyncMode`, `ExecuteAsyncInAsyncMode`).

Example of synchronous code starting a query to be executed asynchronously on the server:
```cs
using (SnowflakeDbConnection conn = new SnowflakeDbConnection("account=testaccount;username=testusername;password=testpassword"))
{
      conn.Open();
      SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand();
      cmd.CommandText = "SELECT ...";
      var queryId = cmd.ExecuteInAsyncMode();
      // ...
}
```

Example of asynchronous code starting a query to be executed asynchronously on the server:
```cs
using (SnowflakeDbConnection conn = new SnowflakeDbConnection("account=testaccount;username=testusername;password=testpassword"))
{
      await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
      SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
      cmd.CommandText = "SELECT ...";
      var queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
      // ...
}
```

You can check the status of a query executed asynchronously on the server either in synchronous code:
```cs
var queryStatus = cmd.GetQueryStatus(queryId);
Assert.IsTrue(conn.IsStillRunning(queryStatus)); // assuming that the query is still running
Assert.IsFalse(conn.IsAnError(queryStatus)); // assuming that the query has not finished with error
```
or the same in an asynchronous code:
```cs
var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
Assert.IsTrue(conn.IsStillRunning(queryStatus)); // assuming that the query is still running
Assert.IsFalse(conn.IsAnError(queryStatus)); // assuming that the query has not finished with error
```

The following example shows how to get query results.
The operation will repeatedly check the query status until the query is completed or timeout happened or reaching the maximum number of attempts.
The synchronous code example:
```cs
DbDataReader reader = cmd.GetResultsFromQueryId(queryId);
```
and the asynchronous code example:
```cs
DbDataReader reader = await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false);
```

**Note**: GET/PUT operations are currently not enabled for asynchronous executions.

## Executing a Batch of SQL Statements (Multi-Statement Support)

With version 2.0.18 and later of the .NET connector, you can send
a batch of SQL statements, separated by semicolons,
to be executed in a single request.

**Note**: Snowflake does not currently support variable binding in multi-statement SQL requests.

---

**Note**

By default, Snowflake returns an error for queries issued with multiple statements to protect against SQL injection attacks. The multiple statements feature makes your system more vulnerable to SQL injections, and so it should be used carefully. You can reduce the risk by using the MULTI_STATEMENT_COUNT parameter to specify the number of statements to be executed, which makes it more difficult to inject a statement by appending to it.

---

You can execute multiple statements as a batch in the same way you execute queries with single statements, except that the query string contains multiple statements separated by semicolons. Note that multiple statements execute sequentially, not in parallel.

You can set this parameter at the session level using the following command:

```
ALTER SESSION SET MULTI_STATEMENT_COUNT = <0/1>;
```

where:

- **0**: Enables an unspecified number of SQL statements in a query.

  Using this value allows batch queries to contain any number of SQL statements without needing to specify the MULTI_STATEMENT_COUNT statement parameter. However, be aware that using this value reduces the protection against SQL injection attacks.

- **1**: Allows one SQL statement or a specified number of statement in a query string (default).

  You must include MULTI_STATEMENT_COUNT as a statement parameter to specify the number of statements included when the query string contains more than one statement. If the number of statements sent in the query string does not match the MULTI_STATEMENT_COUNT value, the .NET driver rejects the request. You can, however, omit this parameter if you send a single statement.

The following example sets the MULTI_STATEMENT_COUNT session parameter to 1. Then for an individual command, it sets MULTI_STATEMENT_COUNT=3 to indicate that the query contains precisely three SQL commands. The query string, `cmd.CommandText` , then contains the three statements to execute.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
	conn.ConnectionString = ConnectionString;
	conn.Open();
	IDbCommand cmd = conn.CreateCommand();
	cmd.CommandText = "ALTER SESSION SET MULTI_STATEMENT_COUNT = 1;";
	cmd.ExecuteNonQuery();
	conn.Close();
}

using (DbCommand cmd = conn.CreateCommand())
{
    // Set statement count
    var stmtCountParam = cmd.CreateParameter();
    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
    stmtCountParam.DbType = DbType.Int16;
    stmtCountParam.Value = 3;
    cmd.Parameters.Add(stmtCountParam);
    cmd.CommandText = "CREATE OR REPLACE TABLE test(n int); INSERT INTO test values(1), (2); SELECT * FROM test ORDER BY n;
    DbDataReader reader = cmd.ExecuteReader();
    do
    {
        if (reader.HasRow)
        {
            while (reader.Read())
            {
                // read data
            }
        }
    }
    while (reader.NextResult());
}
```

## Bind Parameter

**Note**: Snowflake does not currently support variable binding in multi-statement SQL requests.

This example shows how bound parameters are converted from C# data types to
Snowflake data types. For example, if the data type of the Snowflake column
is INTEGER, then you can bind C# data types Int32 or Int16.

This example inserts 3 rows into a table with one column.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "create or replace table T(cola int)";
    int count = cmd.ExecuteNonQuery();
    Assert.AreEqual(0, count);

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "insert into t values (?), (?), (?)";

    var p1 = cmd.CreateParameter();
    p1.ParameterName = "1";
    p1.Value = 10;
    p1.DbType = DbType.Int32;
    cmd.Parameters.Add(p1);

    var p2 = cmd.CreateParameter();
    p2.ParameterName = "2";
    p2.Value = 10000L;
    p2.DbType = DbType.Int32;
    cmd.Parameters.Add(p2);

    var p3 = cmd.CreateParameter();
    p3.ParameterName = "3";
    p3.Value = (short)1;
    p3.DbType = DbType.Int16;
    cmd.Parameters.Add(p3);

    var count = cmd.ExecuteNonQuery();
    Assert.AreEqual(3, count);

    cmd.CommandText = "drop table if exists T";
    count = cmd.ExecuteNonQuery();
    Assert.AreEqual(0, count);

    conn.Close();
}
```

## Bind Array Variables

The sample code creates a table with a single integer column and then uses array binding to populate the table with values 0 to 70000.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
	conn.ConnectionString = ConnectionString;
	conn.Open();

	using (IDbCommand cmd = conn.CreateCommand())
	{
		cmd.CommandText = "create or replace table putArrayBind(colA integer)";
		cmd.ExecuteNonQuery();

		string insertCommand = "insert into putArrayBind values (?)";
		cmd.CommandText = insertCommand;

		int total = 70000;

		List<int> arrint = new List<int>();
		for (int i = 0; i < total; i++)
		{
			arrint.Add(i);
		}
		var p1 = cmd.CreateParameter();
		p1.ParameterName = "1";
		p1.DbType = DbType.Int16;
		p1.Value = arrint.ToArray();
		cmd.Parameters.Add(p1);

		count = cmd.ExecuteNonQuery(); // count = 70000
	}

	conn.Close();
}
```

Binding _an array_ As Variable
------------------------------

Directly binding an array to a variable is not supported currently. Instead, the usual method to pass local arrays as SQL arrays via bind is to use the SQL form `PARSE_JSON(?)`, and then pass a JSON encoded array as string to the variable `?`

Using a stored procedure as an example, which can take an array as an input. Note, you'll need `Newtonsoft.Json` which is already a dependency of the driver.
```cs
using Snowflake.Data;
using Newtonsoft.Json;
..

                    using (IDbCommand cmd = conn.CreateCommand())
                    {

                        int[] vals = new int[] { 1, 2, 3 };
                        string array = JsonConvert.SerializeObject(vals); // alternatively you can do `vals.ToArray()` when passing it to `p1.Value`
                        string sql = "CALL test_db.public.test(parse_json(?))"; // test SP, returns a single value
                        // execute this sql with bind variable 'array'
                        cmd.CommandText = sql;

                        var p1 = cmd.CreateParameter();
                        p1.ParameterName = "1";
                        p1.Value = array; // passing the array in the bind variable. 
                        p1.DbType = DbType.String;
                        cmd.Parameters.Add(p1);

                        IDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            Console.WriteLine(reader.GetString(0));
                        }
                        conn.Close();
                    }
```
