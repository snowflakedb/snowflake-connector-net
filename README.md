Snowflake Connector for .NET
============================

[![Build status](https://ci.appveyor.com/api/projects/status/2gx5agsb7i3m5ije/branch/master?svg=true)](https://ci.appveyor.com/project/howryu/snowflake-connector-net/branch/master)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-connector-net/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-connector-net)


Build
=====

Install
=======

Test
====

Usage
=====

Create Connection
-----------------

To connect to Snowflake, specify a valid connection string, which is key value pairs seperated by semi colon, 
i.e in the format of "\<key1\>=\<value1\>;\<key2\>=\<value2\>...". Valid connection property can be found in 
the following table.

<br />

| Connection Property | Required | Comment                                                                       |
|---------------------|----------|-------------------------------------------------------------------------------|
| ACCOUNT             | Yes      |                                                                               |
| DB                  | No       |                                                                               |
| HOST                | No       | If no value specified, driver will use \<ACCOUNT\>.snowflakecomputing.com     |
| PASSWORD            | Yes      |                                                                               |
| ROLE                | No       |                                                                               |
| SCHEMA              | No       |                                                                               |
| USER                | Yes      |                                                                               |
| WAREHOUSE           | No       |                                                                               |
| CONNECTION_TIMEOUT  | No       | Total timeout in seconds when connecting to Snowflake. Default to 120 seconds |

<br />

Sample code to open a connection to Snowflake:
```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema"

    conn.Open();
    
    conn.Close();
}
```

Run a query and Read data
-------------------------
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

Bind paramter
-------------
```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "insert into t values (?),(?),(?)";
    IDataReader reader = cmd.ExecuteReader();
                  
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
    
    conn.Close();
}
```

Logging
-------
