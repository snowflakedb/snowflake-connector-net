using System;
using System.Data;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Tests.Util;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

#if NET8_0_OR_GREATER
using TaskOrValueTask = System.Threading.Tasks.ValueTask;
#else
using TaskOrValueTask = System.Threading.Tasks.Task;
#endif

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class SFPutFromMemoryIT : SFBaseTestAsync, IDisposable
{
    private readonly SFBaseTestAsyncFixture _fixture;
    private readonly string _schemaName;
    private readonly string _tableName;
    private readonly string _stageName;

    public SFPutFromMemoryIT(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
        _schemaName = _fixture.testConfig.schema;
        var suffix = Guid.NewGuid().ToString("N");
        _tableName = $"PUT_MEM_{suffix}";
        _stageName = $"STAGE_MEM_{suffix}";
    }

    public void Dispose()
    {
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        using var command = conn.CreateCommand();
        command.CommandText = $"DROP STAGE IF EXISTS {_schemaName}.{_stageName}";
        command.ExecuteNonQuery();
        command.CommandText = $"DROP TABLE IF EXISTS {_schemaName}.{_tableName}";
        command.ExecuteNonQuery();
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED, true, 4)]
    [InlineData(SFPutGetTest.StageType.NAMED, false, 4)]
    [InlineData(SFPutGetTest.StageType.TABLE, true, 4)]
    [InlineData(SFPutGetTest.StageType.TABLE, false, 4)]
    [InlineData(SFPutGetTest.StageType.NAMED, true, 200)]
    [InlineData(SFPutGetTest.StageType.NAMED, false, 200)]
    [InlineData(SFPutGetTest.StageType.TABLE, true, 200)]
    [InlineData(SFPutGetTest.StageType.TABLE, false, 200)]
    public async Task TestPutFromMemory_Csv(SFPutGetTest.StageType stageType, bool autoCompress, int rows)
    {
        await InitializeAsync().ConfigureAwait(false);
        var csv = MakeCsvBytes(rows);
        PutFromMemoryAndVerifyRoundTrip(csv, stageType, autoCompress, expectCsvRoundTrip: true, 1);
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED)]
    [InlineData(SFPutGetTest.StageType.TABLE)]
    public async Task TestPutFromMemory_EmptyStream_NoAutoCompress(SFPutGetTest.StageType stageType)
    {
        await InitializeAsync().ConfigureAwait(false);
        PutFromMemoryAndVerifyRoundTrip([], stageType, autoCompress: false, expectCsvRoundTrip: false, 1);
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED)]
    [InlineData(SFPutGetTest.StageType.TABLE)]
    public async Task TestPutFromMemory_EmptyStream_AutoCompress(SFPutGetTest.StageType stageType)
    {
        await InitializeAsync().ConfigureAwait(false);
        PutFromMemoryAndVerifyRoundTrip([], stageType, autoCompress: true, expectCsvRoundTrip: false, 1);
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED, true)]
    [InlineData(SFPutGetTest.StageType.NAMED, false)]
    [InlineData(SFPutGetTest.StageType.TABLE, true)]
    [InlineData(SFPutGetTest.StageType.TABLE, false)]
    public async Task TestPutFromMemory_BinaryData(SFPutGetTest.StageType stageType, bool autoCompress)
    {
        await InitializeAsync().ConfigureAwait(false);
        var binary = new byte[512];
        new Random(42).NextBytes(binary);
        PutFromMemoryAndVerifyRoundTrip(binary, stageType, autoCompress, expectCsvRoundTrip: false, 1);
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED, true)]
    [InlineData(SFPutGetTest.StageType.TABLE, false)]
    public async Task TestPutFromMemory_LargePayload(SFPutGetTest.StageType stageType, bool autoCompress)
    {
        await InitializeAsync().ConfigureAwait(false);
        // ~100 MB payload — exercises size-based code paths without excessive memory use
        var csv = MakeCsvBytes(5_000_000);
        PutFromMemoryAndVerifyRoundTrip(csv, stageType, autoCompress, expectCsvRoundTrip: true, expectedFilesCount: 1);
    }

    [SFFact]
    public async Task TestPutFromMemory_SubsequentUploadsToSameStage()
    {
        await InitializeAsync().ConfigureAwait(false);

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        var stagePath = StagePath(SFPutGetTest.StageType.NAMED);

        // Upload first file — unique PUT filename produces unique dest name on stage
        var csv1 = MakeCsvBytes();
        string destFileName1;
        using (var cmd = (SnowflakeDbCommand)conn.CreateCommand())
        {
            cmd.CommandText = $"PUT file://first.csv {stagePath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE";
            using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(new MemoryStream(csv1));
            Assert.True(reader.Read());
            Assert.Equal("UPLOADED", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            destFileName1 = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);
        }

        // Upload second file — different PUT filename produces a separate file on stage
        var csv2 = MakeCsvBytes(8);
        string destFileName2;
        using (var cmd = (SnowflakeDbCommand)conn.CreateCommand())
        {
            cmd.CommandText = $"PUT file://second.csv {stagePath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE";
            using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(new MemoryStream(csv2));
            Assert.True(reader.Read());
            Assert.Equal("UPLOADED", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            destFileName2 = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);
        }

        // Filenames should be distinct
        Assert.NotEqual(destFileName1, destFileName2);

        // Both files should exist on stage
        VerifyFilesAreUploaded(conn, stagePath, 2);

        // COPY INTO both files and verify combined row count (4 + 8 = 12)
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"TRUNCATE TABLE IF EXISTS {_schemaName}.{_tableName}";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"COPY INTO {_schemaName}.{_tableName} FROM {stagePath}/{destFileName1}";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"COPY INTO {_schemaName}.{_tableName} FROM {stagePath}/{destFileName2}";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"SELECT COUNT(*) FROM {_schemaName}.{_tableName}";
            Assert.Equal(12L, (long)cmd.ExecuteScalar());
        }

        CleanupStageFile(conn, stagePath, destFileName1);
        CleanupStageFile(conn, stagePath, destFileName2);
    }

    [SFTheory]
    [InlineData(SFPutGetTest.StageType.NAMED, true)]
    [InlineData(SFPutGetTest.StageType.NAMED, false)]
    [InlineData(SFPutGetTest.StageType.TABLE, true)]
    [InlineData(SFPutGetTest.StageType.TABLE, false)]
    public async Task TestPutFromMemory_PreCompressedGzip(SFPutGetTest.StageType stageType, bool autoCompress)
    {
        await InitializeAsync().ConfigureAwait(false);
        // Stream is already gzip-compressed; driver should auto-detect and skip double-compression
        var raw = MakeCsvBytes();
        var gzipped = MakeGzipBytes(raw);
        PutFromMemoryAndVerifyUpload(gzipped, stageType, autoCompress, expectedSourceCompression: "gzip");
    }

    [SFFact]
    public async Task TestCallerStreamIsReusableAfterUpload()
    {
        await InitializeAsync().ConfigureAwait(false);
        var data = MakeCsvBytes();
        var stream = new MemoryStream(data);

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        var stagePath = StagePath(SFPutGetTest.StageType.NAMED);
        var fileName = $"{Guid.NewGuid()}.csv";

        string destFileName;
        using (var cmd = (SnowflakeDbCommand)conn.CreateCommand())
        {
            cmd.CommandText = $"PUT file://{fileName} {stagePath} AUTO_COMPRESS=TRUE OVERWRITE=TRUE";
            using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
            Assert.True(reader.Read());
            Assert.Equal("UPLOADED", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            destFileName = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);
        }

        // Verify the caller's stream is still fully readable
        stream.Position = 0;
        var readBack = new byte[stream.Length];
        var bytesRead = stream.Read(readBack, 0, readBack.Length);
        Assert.Equal(data.Length, bytesRead);
        Assert.Equal(data, readBack);

        CleanupStageFile(conn, stagePath, destFileName);
    }

    [SFFact]
    public async Task TestCompressedSizeIsSmallerThanOriginal()
    {
        await InitializeAsync().ConfigureAwait(false);
        // Large enough that gzip makes a real difference
        var csv = MakeCsvBytes(500);
        var stream = new MemoryStream(csv);

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        var stagePath = StagePath(SFPutGetTest.StageType.NAMED);
        var fileName = $"{Guid.NewGuid()}.csv";

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = $"PUT file://{fileName} {stagePath} AUTO_COMPRESS=TRUE OVERWRITE=TRUE";
        using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
        Assert.True(reader.Read());

        var srcSize = long.Parse(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
        var destSize = long.Parse(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        var destFileName = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);
        Assert.Equal(csv.Length, srcSize);
        Assert.True(destSize < srcSize, $"Compressed size {destSize} should be smaller than original {srcSize}");
        Assert.Equal("gzip", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));

        CleanupStageFile(conn, stagePath, destFileName);
    }

    [SFFact]
    public async Task TestQueryIdIsAvailableOnSuccess()
    {
        await InitializeAsync().ConfigureAwait(false);
        var data = MakeCsvBytes();
        var stream = new MemoryStream(data);

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        var stagePath = StagePath(SFPutGetTest.StageType.NAMED);
        var fileName = $"{Guid.NewGuid()}.csv";

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = $"PUT file://{fileName} {stagePath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE";
        using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
        Assert.True(reader.Read());

        var queryId = ((SnowflakeDbDataReader)reader).GetQueryId();
        var destFileName = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);
        Assert.NotNull(queryId);
        Guid.Parse(queryId);
        Assert.Equal(queryId, cmd.GetQueryId());

        var status = cmd.GetQueryStatus(queryId);
        Assert.Equal(QueryStatus.Success, status);

        CleanupStageFile(conn, stagePath, destFileName);
    }

    [SFFact]
    public void TestNullStreamThrows()
    {
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "PUT file://null.csv @~ AUTO_COMPRESS=FALSE";
        Assert.Throws<ArgumentNullException>(() => cmd.ExecuteDbDataReaderWithMemoryStream(null));
    }

    [SFFact]
    public void TestDisposedStreamThrows()
    {
        var stream = new MemoryStream([1, 2, 3]);
        stream.Dispose();

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "PUT file://disposed.csv @~ AUTO_COMPRESS=FALSE";
        Assert.Throws<ObjectDisposedException>(() => cmd.ExecuteDbDataReaderWithMemoryStream(stream));
    }

    [SFFact]
    public void TestStreamAtNonZeroPositionThrows()
    {
        var stream = new MemoryStream([1, 2, 3]);
        stream.Position = 2;

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "PUT file://offset.csv @~ AUTO_COMPRESS=FALSE";
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteDbDataReaderWithMemoryStream(stream));
    }

    [SFFact]
    public async Task TestClosedConnectionThrowsWithoutQueryId()
    {
        await InitializeAsync().ConfigureAwait(false);
        var stream = new MemoryStream([1, 2, 3]);
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        // intentionally NOT opening the connection

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = $"PUT file://closed.csv @~ AUTO_COMPRESS=FALSE";

        var ex = Assert.Throws<SnowflakeDbException>(() => cmd.ExecuteDbDataReaderWithMemoryStream(stream));
        Assert.Null(ex.QueryId);
    }

    [SFFact]
    public async Task TestBadSqlSyntaxThrowsWithQueryId()
    {
        await InitializeAsync().ConfigureAwait(false);
        var stream = new MemoryStream([1, 2, 3]);
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "PUT SOME CODE FORCING SYNTAX ERROR";

        var ex = Assert.Throws<SnowflakeDbException>(() => cmd.ExecuteDbDataReaderWithMemoryStream(stream));
        Assert.NotNull(ex.QueryId);
        Guid.Parse(ex.QueryId);
    }

    [SFFact]
    public async Task TestNonPutCommandWithStreamIgnoresStream()
    {
        await InitializeAsync().ConfigureAwait(false);
        var stream = new MemoryStream("this should be ignored"u8.ToArray());

        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT 1 AS result";
        using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
        Assert.True(reader.Read());
        Assert.Equal("1", reader.GetString(0));
    }

    private void PutFromMemoryAndVerifyRoundTrip(
        byte[] data,
        SFPutGetTest.StageType stageType,
        bool autoCompress,
        bool expectCsvRoundTrip,
        int expectedFilesCount)
    {
        var stream = new MemoryStream(data);
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        var stagePath = StagePath(stageType);
        var fileName = $"{Guid.NewGuid()}.csv";
        var autoCompressFlag = autoCompress ? "TRUE" : "FALSE";

        using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = $"PUT file://{fileName} {stagePath} AUTO_COMPRESS={autoCompressFlag} OVERWRITE=TRUE";
        using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
        Assert.True(reader.Read());
        Assert.Equal("UPLOADED", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
        var errorDetails = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails) ?? "";
        Assert.Empty(errorDetails);

        // Memory stream uploads land on stage as "stream" (or "stream.gz"), not the PUT filename
        var destFileName = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);

        // QueryId must be set
        var queryId = ((SnowflakeDbDataReader)reader).GetQueryId();
        Assert.NotNull(queryId);
        Guid.Parse(queryId);
        Assert.Equal(queryId, cmd.GetQueryId());

        switch (autoCompress)
        {
            // Compression assertions
            case true when data.Length > 0:
                Assert.Equal("none", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                Assert.Equal("gzip", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                break;
            case false:
                Assert.Equal("none", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                Assert.Equal("none", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                break;
        }

        // Verify file is on stage
        VerifyFilesAreUploaded(conn, stagePath, expectedFilesCount);

        // COPY INTO → SELECT round-trip (only for valid CSV data)
        if (expectCsvRoundTrip && data.Length > 0)
        {
            var expectedRows = Encoding.UTF8.GetString(data).Split('\n').Count(l => !string.IsNullOrEmpty(l));
            CopyIntoAndVerify(conn, stageType, stagePath, destFileName, expectedRows);
        }

        CleanupStageFile(conn, stagePath, destFileName);
    }

    private void PutFromMemoryAndVerifyUpload(
        byte[] data,
        SFPutGetTest.StageType stageType,
        bool autoCompress,
        string expectedSourceCompression)
    {
        var stream = new MemoryStream(data);
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();

        var stagePath = StagePath(stageType);
        var fileName = $"{Guid.NewGuid()}.csv";
        var autoCompressFlag = autoCompress ? "TRUE" : "FALSE";

        string destFileName;
        using (var cmd = (SnowflakeDbCommand)conn.CreateCommand())
        {
            cmd.CommandText = $"PUT file://{fileName} {stagePath} AUTO_COMPRESS={autoCompressFlag} OVERWRITE=TRUE";
            using var reader = cmd.ExecuteDbDataReaderWithMemoryStream(stream);
            Assert.True(reader.Read());
            Assert.Equal("UPLOADED", reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Empty(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails) ?? "");
            destFileName = reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName);

            if (autoCompress)
                Assert.Equal(expectedSourceCompression, reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
        }

        CleanupStageFile(conn, stagePath, destFileName);
    }

    private void CopyIntoAndVerify(
        SnowflakeDbConnection conn,
        SFPutGetTest.StageType stageType,
        string stagePath,
        string destFileName,
        int expectedRows)
    {
        using var command = conn.CreateCommand();

        // Memory stream uploads land on stage as "stream" (or "stream.gz" with auto-compress),
        // not as the filename from the PUT command. Use the actual dest name for COPY INTO.
        command.CommandText = stageType == SFPutGetTest.StageType.TABLE
            ? $"COPY INTO {_schemaName}.{_tableName}"
            : $"COPY INTO {_schemaName}.{_tableName} FROM {stagePath}/{destFileName}";
        command.ExecuteNonQuery();

        // Verify row count
        command.CommandText = $"SELECT COUNT(*) FROM {_schemaName}.{_tableName}";
        var count = (long)command.ExecuteScalar();
        Assert.Equal(expectedRows, count);

        // Verify data content
        command.CommandText = $"SELECT * FROM {_schemaName}.{_tableName} LIMIT 1";
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        for (var i = 0; i < s_colData.Length; i++)
            Assert.Equal(s_colData[i], reader.GetString(i));
    }

    private static void VerifyFilesAreUploaded(SnowflakeDbConnection conn, string stage, int expectedFilesCount)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"LIST {stage}";
        using var reader = cmd.ExecuteReader();
        var dt = new DataTable();
        dt.Load(reader);
        Assert.Equal(expectedFilesCount, dt.Rows.Count);
    }

    private static void CleanupStageFile(SnowflakeDbConnection conn, string stagePath, string destFileName)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"REMOVE {stagePath}/{destFileName}";
        try { cmd.ExecuteNonQuery(); } catch { /* best-effort cleanup */ }
    }

    private async TaskOrValueTask InitializeAsync()
    {
        using var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        using var command = conn.CreateCommand();

        command.CommandText = $"CREATE OR REPLACE TABLE {_schemaName}.{_tableName} (C1 STRING, C2 STRING, C3 STRING)";
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);

        command.CommandText = $"CREATE OR REPLACE STAGE {_schemaName}.{_stageName}";
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static readonly string[] s_colData = ["FIRST", "SECOND", "THIRD"];

    private static byte[] MakeCsvBytes(int rows = 4)
    {
        var row = $"{string.Join(",", s_colData)}\n";
        return Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(row, rows)));
    }

    private static byte[] MakeGzipBytes(byte[] raw)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionMode.Compress, leaveOpen: true))
            gzip.Write(raw, 0, raw.Length);
        return output.ToArray();
    }

    private string StagePath(SFPutGetTest.StageType stageType) => stageType switch
    {
        SFPutGetTest.StageType.TABLE => $"@{_schemaName}.%{_tableName}",
        SFPutGetTest.StageType.NAMED => $"@{_schemaName}.{_stageName}",
        _ => throw new ArgumentOutOfRangeException(nameof(stageType))
    };
}
