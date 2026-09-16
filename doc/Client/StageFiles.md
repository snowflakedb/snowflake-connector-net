## PUT local files to stage

PUT command can be used to upload files of a local directory or a single local file to the Snowflake stages (named, internal table stage or internal user stage).
It also supports uploading data from an in-memory stream via `ExecuteDbDataReaderWithMemoryStream`.
Such staging files can be used to load data into a table.
More on this topic: [File staging with PUT](https://docs.snowflake.com/en/sql-reference/sql/put).

In the driver the command can be executed in the following way:

```cs
using var conn = new SnowflakeDbConnection();
try
{
    conn.ConnectionString = "<connection parameters>";
    await conn.OpenAsync(cancellationToken);
    var cmd = (SnowflakeDbCommand)conn.CreateCommand(); // cast allows get QueryId from the command

    cmd.CommandText = "PUT file://some_data.csv @my_schema.my_stage AUTO_COMPRESS=TRUE";
	await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
	Assert.IsTrue(await reader.ReadAsync(cancellationToken));
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

### PUT from in-memory stream

Instead of uploading a local file, data can be uploaded directly from a `MemoryStream`.
Cast the command to `SnowflakeDbCommand` and call `ExecuteDbDataReaderWithMemoryStream`:

```cs
using var conn = new SnowflakeDbConnection();
conn.ConnectionString = "<connection parameters>";
await conn.OpenAsync(cancellationToken);

var cmd = (SnowflakeDbCommand)conn.CreateCommand();
cmd.CommandText = "PUT file://data.csv @my_schema.my_stage AUTO_COMPRESS=TRUE";

using var stream = new MemoryStream(Encoding.UTF8.GetBytes("col1,col2\n1,hello\n2,world"));
using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
Assert.IsTrue(reader.Read());
```

The caller owns the `MemoryStream` and is responsible for disposing it.
The file name in the PUT command (`file://data.csv`) determines the destination stage name
using the format `stream.<filename>` (e.g. `stream.data.csv.gz`). Any directory components
in the path are stripped — only the file name is used.
When `AUTO_COMPRESS=TRUE`, compression is performed in-memory by default.
To force temporary-file-based compression instead, set the environment variable
`SF_PUT_DISABLE_IN_MEMORY_COMPRESS=true` (see [Environment Variables](EnvironmentVariables.md)).

#### Encryption and disk spill

When uploading to an encrypted stage, the driver encrypts the data before sending it.
By default, the encryption step uses an in-memory buffer that spills to a temporary file
once the payload exceeds the `FILE_TRANSFER_MEMORY_THRESHOLD` connection parameter
(default: 1 MB). The temporary file is created inside a dedicated directory that is
automatically cleaned up after the upload completes.

To keep the entire upload pipeline — compression and encryption — fully in memory
with no disk I/O, set `FILE_TRANSFER_MEMORY_THRESHOLD=-1` in the connection string:

```cs
conn.ConnectionString = "ACCOUNT=myaccount;USER=myuser;PASSWORD=mypassword;FILE_TRANSFER_MEMORY_THRESHOLD=-1;";
```

| `FILE_TRANSFER_MEMORY_THRESHOLD` value | Behavior |
|---------------------------------------|----------|
| `-1` | Encryption stays entirely in memory (no temp files) |
| `N` (positive integer) | Encryption spills to a temp file when the payload exceeds N bytes |
| _(not set)_ | Defaults to 1 MB (1048576 bytes) |

## GET stage files

GET command allows downloading stage directories or files to a local directory.
It can be used in connection with named stage, table internal stage or user stage.
Detailed information on the command: [Downloading files with GET](https://docs.snowflake.com/en/sql-reference/sql/get).

To use the command in the driver, similar code can be executed in a client app:

```cs
    try
    {
	    conn.ConnectionString = "<connection parameters>";
	    await conn.OpenAsync(cancellationToken);
	    var cmd = (SnowflakeDbCommand)conn.CreateCommand(); // cast allows get QueryId from the command

	    cmd.CommandText = "GET @my_schema.my_stage/stage_file.csv file://local_file.csv AUTO_COMPRESS=TRUE";
	    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
	    Assert.IsTrue(await reader.ReadAsync(cancellationToken)); // True on success, False if failure
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
