using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests;

[CollectionDefinition(nameof(ChunkDownloadCancellationTestCollection), DisableParallelization = true)]
public class ChunkDownloadCancellationTestCollection : ICollectionFixture<ChunkDownloadCancellationTestCollection>
{
}

[Collection(nameof(ChunkDownloadCancellationTestCollection))]
public sealed class ChunkDownloadCancellationTest : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public ChunkDownloadCancellationTest(SFBaseTestAsyncFixture fixture, ChunkDownloadCancellationTestCollection _) : base(fixture)
    {
        _fixture = fixture;
    }

    [SFFact]
    public async Task TestCancellationDuringJsonChunkDownloadThrows()
    {
        // Generate enough data to require chunked results (multiple chunks)
        const int TestRowCount = 20000;
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
            await _fixture.CreateOrReplaceTable(conn, tableName, ["col STRING"]).ConfigureAwait(false);

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"insert into {tableName}(select randstr(200, random()) from table(generator(rowcount => {TestRowCount})))";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            cmd.CommandText = $"select * from {tableName}";
            using var cts = new CancellationTokenSource();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var reader = await cmd.ExecuteReaderAsync(cts.Token).ConfigureAwait(false);
                cts.CancelAfter(TimeSpan.FromMilliseconds(100));
                while (await reader.ReadAsync(cts.Token).ConfigureAwait(false))
                {
                    // Read through all results - should be interrupted by cancellation
                    _ = reader.GetString(0);
                }
            });
        }
        finally
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
        }
    }

    [SFFact]
    public async Task TestCancellationDuringArrowChunkDownloadThrows()
    {
        const int TestRowCount = 20000;
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            SessionParameterAlterer.SetResultFormat(conn, ResultFormat.ARROW);
            await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "col STRING" }).ConfigureAwait(false);

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"insert into {tableName}(select randstr(200, random()) from table(generator(rowcount => {TestRowCount})))";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            cmd.CommandText = $"select * from {tableName}";
            using var cts = new CancellationTokenSource();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var reader = await cmd.ExecuteReaderAsync(cts.Token).ConfigureAwait(false);
                cts.CancelAfter(TimeSpan.FromMilliseconds(100));
                while (await reader.ReadAsync(cts.Token).ConfigureAwait(false))
                {
                    _ = reader.GetString(0);
                }
            });
        }
        finally
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
        }
    }

    [SFFact]
    public async Task TestPreCancelledTokenThrowsBeforeChunkDownload()
    {
        const int TestRowCount = 20000;
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");

        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            SessionParameterAlterer.SetResultFormat(conn, ResultFormat.JSON);
            await _fixture.CreateOrReplaceTable(conn, tableName, ["col STRING"]).ConfigureAwait(false);

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"insert into {tableName}(select randstr(200, random()) from table(generator(rowcount => {TestRowCount})))";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            cmd.CommandText = $"select * from {tableName}";

            // Use an already-cancelled token
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => cmd.ExecuteReaderAsync(cts.Token));
        }
        finally
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
        }
    }
}
