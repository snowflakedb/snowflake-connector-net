using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Snowflake.Data.Tests.Util.Shims;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class ArrowChunkParserTest
{
    [SFTheory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task TestParseChunkReadsRecordBatches(int numberOfRecordBatch)
    {
        // Arrange
        using var stream = CreateArrowStream(numberOfRecordBatch, i => 10 * i);

        var parser = new ArrowChunkParser(stream);

        // Act
        var chunk = new ArrowResultChunk(1);
        await parser.ParseChunkAsync(chunk, CancellationToken.None).ConfigureAwait(false);

        // Assert
        Assert.Equal(numberOfRecordBatch, chunk.RecordBatch.Count);
        for (var i = 0; i < numberOfRecordBatch; i++)
        {
            var numberOfRecordsInBatch = 10 * i;
            Assert.Equal(numberOfRecordsInBatch, chunk.RecordBatch[i].Length);
        }
    }

    [SFFact]
    public async Task TestParseChunkThrowsWhenTokenIsPreCancelled()
    {
        var stream = CreateArrowStream(batchCount: 1, _ => 5);
        var parser = new ArrowChunkParser(stream);
        var chunk = new ArrowResultChunk(1);

        var cts = new CancellationTokenSource();
        await cts.CancelAsync().ConfigureAwait(false);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => parser.ParseChunkAsync(chunk, cts.Token)).ConfigureAwait(false);
    }

    [SFFact]
    public async Task TestParseChunkCompletesWithNonCancelledToken()
    {
        const int BatchCount = 3;
        const int RowsPerBatch = 10;
        var stream = CreateArrowStream(BatchCount, _ => RowsPerBatch);
        var parser = new ArrowChunkParser(stream);
        var chunk = new ArrowResultChunk(1);

        using var cts = new CancellationTokenSource();
        await parser.ParseChunkAsync(chunk, cts.Token).ConfigureAwait(false);

        Assert.Equal(BatchCount, chunk.RecordBatch.Count);
        for (var i = 0; i < BatchCount; i++)
            Assert.Equal(RowsPerBatch, chunk.RecordBatch[i].Length);
    }

    [SFFact]
    public async Task TestParseChunkThrowsWhenCancelledDuringMultipleBatches()
    {
        // Create a stream that will cancel after reading the 1/3 of content
        var cts = new CancellationTokenSource();
        using var arrowStream = CreateArrowStream(batchCount: 20, _ => 1_000);
        var arrowData = arrowStream.ToArray();
        var stream = new CancellingReadStream(arrowData, cts, bytesBeforeCancel: arrowData.Length / 3);

        var parser = new ArrowChunkParser(stream);
        var chunk = new ArrowResultChunk(1);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parser.ParseChunkAsync(chunk, cts.Token)).ConfigureAwait(false);
    }

    private static MemoryStream CreateArrowStream(int batchCount, Func<int, int> rowsPerBatch)
    {
        var stream = new MemoryStream();
        for (var i = 0; i < batchCount; i++)
        {
            var recordCol = Enumerable.Range(1, rowsPerBatch.Invoke(i));
            var recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(recordCol)))
                .Build();

            var writer = new ArrowStreamWriter(stream, recordBatch.Schema, leaveOpen: true);
            writer.WriteRecordBatch(recordBatch);
        }

        stream.Position = 0;
        return stream;
    }
}
