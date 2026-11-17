using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

using TaskOrValueTask = System.Threading.Tasks.
#if NET8_0_OR_GREATER
ValueTask;
#else
Task;
#endif

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class MaxLobSizeITTestFixture : SFBaseTestAsyncFixture
{
    internal readonly string OutputDirectory;

    public MaxLobSizeITTestFixture()
    {
        // Create temp output directory for downloaded files
        OutputDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(OutputDirectory);
    }

    public override TaskOrValueTask DisposeAsync()
    {
        // Delete temp output directory and downloaded files
        Directory.Delete(OutputDirectory, true);
        return base.DisposeAsync();
    }
}

public class MaxLobSizeIT : SFBaseTestAsync, IDisposable, IClassFixture<MaxLobSizeITTestFixture>
{
    private readonly SFBaseTestAsyncFixture _fixture;

    private ResultFormat _resultFormat;
    private readonly string _outputDirectory;

    //private const int MaxLobSize = (128 * 1024 * 1024); // new max LOB size
    private const int MaxLobSize = (16 * 1024 * 1024); // current max LOB size
    private const int MediumSize = (MaxLobSize / 4);
    private const int OriginSize = (MediumSize / 2);
    private const int LobRandomRange = 100000 + 1; // range to use for generating random numbers (0 - 100000)

    private static readonly string[] s_colName = { "C1", "C2", "C3" };
    private static readonly AsyncLocal<string> s_tTableName = new();
    private static readonly AsyncLocal<string> s_tInsertQuery = new();
    private static readonly AsyncLocal<string> s_tPositionalBindingInsertQuery = new();
    private static readonly AsyncLocal<string> s_tNamedBindingInsertQuery = new();
    private static readonly AsyncLocal<string> s_tSelectQuery = new();
    private static readonly AsyncLocal<string> s_tFileName = new();
    private static readonly AsyncLocal<string> s_tInputFilePath = new();
    private static readonly AsyncLocal<string> s_tOutputFilePath = new();
    private static readonly AsyncLocal<List<string>> s_tFilesToDelete = new();
    private static readonly AsyncLocal<string[]> s_tColData = new();

    public MaxLobSizeIT(MaxLobSizeITTestFixture maxLobeFixture) : base(maxLobeFixture)
    {
        _fixture = maxLobeFixture;
        _outputDirectory = maxLobeFixture.OutputDirectory;
        var threadSuffix = Guid.NewGuid().ToString("N");

        s_tTableName.Value = $"LOB_TABLE_{threadSuffix}";
        var tableNameWithColumns = $"{s_tTableName.Value}({string.Join(", ", s_colName.Select(col => col))})";
        s_tInsertQuery.Value = $"INSERT INTO {tableNameWithColumns} VALUES ";
        s_tPositionalBindingInsertQuery.Value = $"INSERT INTO {tableNameWithColumns} VALUES (?, ?, ?)";
        s_tNamedBindingInsertQuery.Value = $"INSERT INTO {tableNameWithColumns} VALUES (:1, :2, :3)";
        s_tSelectQuery.Value = $"SELECT * FROM {s_tTableName.Value}";
        s_tFilesToDelete.Value = new List<string>();

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            conn.Open();
            using (var command = conn.CreateCommand())
            {
                // Create temp table
                var columnNamesWithTypes = $"{s_colName[0]} VARCHAR({MaxLobSize}), {s_colName[1]} VARCHAR({MaxLobSize}), {s_colName[2]} INT";
                command.CommandText = $"CREATE OR REPLACE TABLE {s_tTableName.Value} ({columnNamesWithTypes})";
                command.ExecuteNonQuery();
            }
        }
    }

    public void Dispose()
    {
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            conn.OpenAsync(CancellationToken.None);
            using (var command = conn.CreateCommand())
            {
                // Drop temp table
                command.CommandText = $"DROP TABLE IF EXISTS {s_tTableName.Value}";
                command.ExecuteNonQueryAsync();
            }
        }

        if (s_tFilesToDelete != null)
        {
            foreach (var file in s_tFilesToDelete.Value)
            {
                File.Delete(file);
            }
        }
    }

    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(SelectOnSpecifiedSizeTestCases))]
    public async Task TestSelectOnSpecifiedSize(ResultFormat resultFormat, int size)
    {
        // arrange
        _resultFormat = resultFormat;

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            using (var command = conn.CreateCommand())
            {
                // act
                command.CommandText = $"SELECT RANDSTR({size}, 124)";
                var row = (string)await command.ExecuteScalarAsync(CancellationToken).ConfigureAwait(false);

                // assert
                Assert.Equal(size, row.Length);
            }
        }
    }


    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(LiteralInsertTestCases))]
    public async Task TestLiteralInsert(ResultFormat resultFormat, int lobSize)
    {
        // arrange
        _resultFormat = resultFormat;
        var c1 = GenerateRandomString(lobSize);
        var c2 = GenerateRandomString(lobSize);
        var c3 = new Random().Next(LobRandomRange);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

            using (var command = conn.CreateCommand())
            {
                // act
                command.CommandText = $"{s_tInsertQuery.Value} ('{c1}', '{c2}', '{c3}')";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                command.CommandText = s_tSelectQuery.Value;
                var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                // assert
                Assert.True(reader.Read());
                Assert.Equal(c1, reader.GetString(0));
                Assert.Equal(c2, reader.GetString(1));
                Assert.Equal(c3, reader.GetInt64(2));
                CheckColumnMetadata(reader);
            }
        }
    }

    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(PositionalInsertTestCases))]
    public async Task TestPositionalInsert(ResultFormat resultFormat, int lobSize)
    {
        // arrange
        _resultFormat = resultFormat;
        var c1 = GenerateRandomString(lobSize);
        var c2 = GenerateRandomString(lobSize);
        var c3 = new Random().Next(LobRandomRange);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

            using (var command = conn.CreateCommand())
            {
                // act
                command.CommandText = $"{s_tPositionalBindingInsertQuery.Value}";

                var p1 = command.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.String;
                p1.Value = c1;
                command.Parameters.Add(p1);

                var p2 = command.CreateParameter();
                p2.ParameterName = "2";
                p2.DbType = DbType.String;
                p2.Value = c2;
                command.Parameters.Add(p2);

                var p3 = command.CreateParameter();
                p3.ParameterName = "3";
                p3.DbType = DbType.UInt32;
                p3.Value = c3;
                command.Parameters.Add(p3);

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                command.CommandText = s_tSelectQuery.Value;
                var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                // assert
                Assert.True(reader.Read());
                Assert.Equal(c1, reader.GetString(0));
                Assert.Equal(c2, reader.GetString(1));
                Assert.Equal(c3, reader.GetInt64(2));
                CheckColumnMetadata(reader);
            }
        }
    }


    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(NamedInsertTestCases))]
    public async Task TestNamedInsert(ResultFormat resultFormat, int lobSize)
    {
        // arrange
        _resultFormat = resultFormat;
        var c1 = GenerateRandomString(lobSize);
        var c2 = GenerateRandomString(lobSize);
        var c3 = new Random().Next(LobRandomRange);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

            using (var command = conn.CreateCommand())
            {
                // act
                command.CommandText = $"{s_tNamedBindingInsertQuery.Value}";

                var p1 = command.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.String;
                p1.Value = c1;
                command.Parameters.Add(p1);

                var p2 = command.CreateParameter();
                p2.ParameterName = "2";
                p2.DbType = DbType.String;
                p2.Value = c2;
                command.Parameters.Add(p2);

                var p3 = command.CreateParameter();
                p3.ParameterName = "3";
                p3.DbType = DbType.UInt32;
                p3.Value = c3;
                command.Parameters.Add(p3);

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                command.CommandText = s_tSelectQuery.Value;
                var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                // assert
                Assert.True(reader.Read());
                Assert.Equal(c1, reader.GetString(0));
                Assert.Equal(c2, reader.GetString(1));
                Assert.Equal(c3, reader.GetInt64(2));
                CheckColumnMetadata(reader);
            }
        }
    }

    [SFTheory(SkipCondition.SkipOnJenkins, retriesCount: RetriesCount.Thrice), MemberData(nameof(PutGetCommandTestCases))]
    public async Task TestPutGetCommand(ResultFormat resultFormat, int lobSize)
    {
        // arrange
        _resultFormat = resultFormat;
        var c1 = GenerateRandomString(lobSize);
        var c2 = GenerateRandomString(lobSize);
        var c3 = new Random().Next(LobRandomRange);
        s_tColData.Value = new string[] { c1, c2, c3.ToString() };

        PrepareTest();

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await AlterSessionSettingsAsync(conn).ConfigureAwait(false);

            await PutFile(conn).ConfigureAwait(false);
            await CopyIntoTableAsync(conn).ConfigureAwait(false);
            await GetFileAsync(conn).ConfigureAwait(false);
        }
    }

    private static readonly IEnumerable<int> s_lobSizeTestCases = new[]
    {
        OriginSize,
        MediumSize,
        MaxLobSize
    };

    private static IEnumerable<ResultFormat> ResultFormats => new[]
        { ResultFormat.ARROW, ResultFormat.JSON };

    private static IEnumerable<object[]> CombinedTestCases(string baseTestName)
    {
        foreach (var resultFormat in ResultFormats)
        {
            foreach (var lobSize in s_lobSizeTestCases)
            {
                yield return new object[] { resultFormat, lobSize };
            }
        }
    }

    public static IEnumerable<object[]> SelectOnSpecifiedSizeTestCases() =>
        CombinedTestCases(nameof(TestSelectOnSpecifiedSize));

    public static IEnumerable<object[]> LiteralInsertTestCases() =>
        CombinedTestCases(nameof(TestLiteralInsert));

    public static IEnumerable<object[]> PositionalInsertTestCases() =>
        CombinedTestCases(nameof(TestPositionalInsert));

    public static IEnumerable<object[]> NamedInsertTestCases() =>
        CombinedTestCases(nameof(TestNamedInsert));

    public static IEnumerable<object[]> PutGetCommandTestCases() =>
        CombinedTestCases(nameof(TestPutGetCommand));

    private void PrepareTest()
    {
        s_tFileName.Value = $"{Guid.NewGuid()}.csv";
        s_tInputFilePath.Value = Path.GetTempPath() + s_tFileName.Value;

        var data = $"{s_tColData.Value[0]},{s_tColData.Value[1]},{s_tColData.Value[2]}";
        using (var stream = FileOperations.Instance.Create(s_tInputFilePath.Value))
        using (var writer = new StreamWriter(stream))
        {
            writer.Write(data);
        }

        s_tOutputFilePath.Value = $@"{_outputDirectory}/{s_tFileName.Value}";
        s_tFilesToDelete.Value.Add(s_tInputFilePath.Value);
        s_tFilesToDelete.Value.Add(s_tOutputFilePath.Value);
    }

    private static async Task PutFile(SnowflakeDbConnection conn)
    {
        using (var command = conn.CreateCommand())
        {
            // arrange
            command.CommandText = $"PUT file://{s_tInputFilePath.Value} @%{s_tTableName.Value} AUTO_COMPRESS=FALSE";

            // act
            var reader = command.ExecuteReader();

            // assert
            Assert.True(await reader.ReadAsync().ConfigureAwait(false));
            Assert.Equal(ResultStatus.UPLOADED.ToString(),
                reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
        }
    }

    private static async Task CopyIntoTableAsync(SnowflakeDbConnection conn)
    {
        using (var command = conn.CreateCommand())
        {
            // arrange
            command.CommandText = $"COPY INTO {s_tTableName.Value}";

            // act
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // arrange
            command.CommandText = $"SELECT * FROM {s_tTableName.Value}";

            // act
            var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            // assert
            Assert.True(await reader.ReadAsync().ConfigureAwait(false));
            AssertOnColData(reader, 0);
            AssertOnColData(reader, 1);
            AssertOnColData(reader, 2);
            CheckColumnMetadata(reader);
        }
    }

    private static void AssertOnColData(DbDataReader reader, int index)
    {
        var actual = reader.GetString(index);
        var substringLength = s_tColData.Value[index].Length > 20 ? 20 : s_tColData.Value[index].Length;
        Assert.True(s_tColData.Value[index] == actual, $"Expected strings to be equal. Expected: '{s_tColData.Value[index].Substring(0, substringLength)}' [...], got '{actual}' instead.");
    }

    private async Task GetFileAsync(DbConnection conn)
    {
        using (var command = conn.CreateCommand())
        {
            // arrange
            command.CommandText = $"GET @%{s_tTableName.Value}/{s_tFileName.Value} file://{_outputDirectory}";

            // act
            var reader = command.ExecuteReader();

            // assert
            Assert.True(await reader.ReadAsync().ConfigureAwait(false));
            Assert.Equal(ResultStatus.DOWNLOADED.ToString(),
                reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

            // arrange
            var content = File.ReadAllText(s_tOutputFilePath.Value).Split(',');

            // assert
            Assert.Equal(s_tColData.Value[0], content[0]);
            Assert.Equal(s_tColData.Value[1], content[1]);
            Assert.Equal(s_tColData.Value[2], content[2]);
        }
    }

    private async Task AlterSessionSettingsAsync(SnowflakeDbConnection conn)
    {
        using (var command = conn.CreateCommand())
        {
            // Alter session result format
            command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {_resultFormat}";
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            //// Alter session max lob
            //command.CommandText = "ALTER SESSION SET FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
            //await command.ExecuteNonQueryAsync();
            //command.CommandText = "alter session set ALLOW_LARGE_LOBS_IN_EXTERNAL_SCAN = true";
            //await command.ExecuteNonQueryAsync();
        }
    }

    private static void CheckColumnMetadata(DbDataReader reader)
    {
        var dataTable = reader.GetSchemaTable();

        var row = dataTable.Rows[0];
        Assert.Equal(s_colName[0], row[SchemaTableColumn.ColumnName]);
        Assert.Equal(0, row[SchemaTableColumn.ColumnOrdinal]);
        Assert.Equal(MaxLobSize, row[SchemaTableColumn.ColumnSize]);
        Assert.Equal(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);

        row = dataTable.Rows[1];
        Assert.Equal(s_colName[1], row[SchemaTableColumn.ColumnName]);
        Assert.Equal(1, row[SchemaTableColumn.ColumnOrdinal]);
        Assert.Equal(MaxLobSize, row[SchemaTableColumn.ColumnSize]);
        Assert.Equal(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);

        row = dataTable.Rows[2];
        Assert.Equal(s_colName[2], row[SchemaTableColumn.ColumnName]);
        Assert.Equal(2, row[SchemaTableColumn.ColumnOrdinal]);
        Assert.Equal(0, row[SchemaTableColumn.ColumnSize]);
        Assert.Equal(SFDataType.FIXED, (SFDataType)row[SchemaTableColumn.ProviderType]);
    }

    private static string GenerateRandomString(int sizeInBytes)
    {
        var bufferSize = sizeInBytes / 2;
        var rand = new Random();
        var bytes = new byte[bufferSize];
        rand.NextBytes(bytes);

        // Convert to hex and remove the '-' character for the correct string length
        return BitConverter.ToString(bytes).Replace("-", "");
    }
}
