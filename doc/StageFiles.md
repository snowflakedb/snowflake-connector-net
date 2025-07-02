## PUT local files to stage

PUT command can be used to upload files of a local directory or a single local file to the Snowflake stages (named, internal table stage or internal user stage).
Such staging files can be used to load data into a table.
More on this topic: [File staging with PUT](https://docs.snowflake.com/en/sql-reference/sql/put).

In the driver the command can be executed in a bellow way:

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    try
    {
	    conn.ConnectionString = "<connection parameters>";
	    conn.Open();
	    var cmd = (SnowflakeDbCommand)conn.CreateCommand(); // cast allows get QueryId from the command

	    cmd.CommandText = "PUT file://some_data.csv @my_schema.my_stage AUTO_COMPRESS=TRUE";
	    var reader = cmd.ExecuteReader();
	    Assert.IsTrue(reader.Read());
        Assert.DoesNotThrow(() => Guid.Parse(cmd.GetQueryId()));
    }
    catch (SnowflakeDbException e)
    {
        Assert.DoesNotThrow(() => Guid.Parse(e.QueryId)); // when failed
        Assert.That(e.InnerException.GetType(), Is.EqualTo(typeof(FileNotFoundException)));
    }
```

In case of a failure a SnowflakeDbException exception will be thrown with affected QueryId if possible.
If it was after the query got executed this exception will be a SnowflakeDbException containing affected QueryId.
In case of the initial phase of execution QueryId might not be provided.
Inner exception (if applicable) will provide some details on the failure cause and
it will be for example: FileNotFoundException, DirectoryNotFoundException.

## GET stage files

GET command allows to download stage directories or files to a local directory.
It can be used in connection with named stage, table internal stage or user stage.
Detailed information on the command: [Downloading files with GET](https://docs.snowflake.com/en/sql-reference/sql/get).

To use the command in a driver similar code can be executed in a client app:

```cs
    try
    {
	    conn.ConnectionString = "<connection parameters>";
	    conn.Open();
	    var cmd = (SnowflakeDbCommand)conn.CreateCommand(); // cast allows get QueryId from the command

	    cmd.CommandText = "GET @my_schema.my_stage/stage_file.csv file://local_file.csv AUTO_COMPRESS=TRUE";
	    var reader = cmd.ExecuteReader();
	    Assert.IsTrue(reader.Read()); // True on success, False if failure
        Assert.DoesNotThrow(() => Guid.Parse(cmd.GetQueryId()));
    }
    catch (SnowflakeDbException e)
    {
        Assert.DoesNotThrow(() => Guid.Parse(e.QueryId)); // on failure
    }
```

In case of a failure a SnowflakeDbException will be thrown with affected QueryId if possible.
When no technical or syntax errors occurred but the DBDataReader has no data to process it returns False
without throwing an exception.
