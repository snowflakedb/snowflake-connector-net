using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

#if !NET8_0_OR_GREATER
using Snowflake.Data.Tests.IntegrationTests;
#endif

namespace Snowflake.Data.Tests.UnitTests;

public sealed class ReusableChunkParserTest
{
    [SFFact]
    public async Task TestParseChunkThrowsWhenTokenIsPreCancelled()
    {
        var data = "[ [\"1\", \"2\"],  [\"3\", \"4\"] ]";
        var bytes = Encoding.UTF8.GetBytes(data);
        var stream = new MemoryStream(bytes);
        var parser = new ReusableChunkParser(stream);
        var chunk = CreateChunk(2, 2);

        var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => parser.ParseChunkAsync(chunk, cts.Token));
    }

    [SFFact]
    public async Task TestParseChunkThrowsWhenTokenCancelledDuringParsing()
    {
        // Use a stream that blocks until cancellation is triggered
        var cts = new CancellationTokenSource();
        var data = Encoding.UTF8.GetBytes($"[ {string.Join(", ", GenerateRows(50000))} ]");
        var stream = new CancellingReadStream(data, cts, 5);
        var parser = new ReusableChunkParser(stream);
        var chunk = CreateChunk(1, 100);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => parser.ParseChunkAsync(chunk, cts.Token));
    }

    [SFFact]
    public async Task TestParseChunkCompletesWithNonCancelledToken()
    {
        var data = "[ [\"hello\", \"world\"],  [\"foo\", \"bar\"] ]";
        var bytes = Encoding.UTF8.GetBytes(data);
        var stream = new MemoryStream(bytes);
        var parser = new ReusableChunkParser(stream);
        var chunk = CreateChunk(2, 2);

        using var cts = new CancellationTokenSource();
        await parser.ParseChunkAsync(chunk, cts.Token);

        chunk.Next();
        Assert.Equal("hello", chunk.ExtractCell(0).SafeToString());
        Assert.Equal("world", chunk.ExtractCell(1).SafeToString());
        chunk.Next();
        Assert.Equal("foo", chunk.ExtractCell(0).SafeToString());
        Assert.Equal("bar", chunk.ExtractCell(1).SafeToString());
    }

    [SFFact]
    public async Task TestParseChunkThrowsWhenCancelledDuringLargePayload()
    {
        // Generate a large payload that takes time to parse
        var sb = new StringBuilder("[ ");
        for (var i = 0; i < 100000; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"[\"{new string('x', 100)}\"]");
        }
        sb.Append(" ]");

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        using var cts = new CancellationTokenSource();

        // Use a slow stream that triggers cancellation after some bytes
        var stream = new CancellingReadStream(bytes, cts, bytes.Length / 4);
        var parser = new ReusableChunkParser(stream);
        var chunk = CreateChunk(1, 100000);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => parser.ParseChunkAsync(chunk, cts.Token));
    }

    [SFFact]
    public async Task TestParseChunkWithCancellationTokenNoneCompletesNormally()
    {
        var data = "[ [null, \"test\"],  [\"value\", null] ]";
        var bytes = Encoding.UTF8.GetBytes(data);
        var stream = new MemoryStream(bytes);
        var parser = new ReusableChunkParser(stream);
        var chunk = CreateChunk(2, 2);

        await parser.ParseChunkAsync(chunk, CancellationToken.None);

        chunk.Next();
        Assert.Null(chunk.ExtractCell(0).SafeToString());
        Assert.Equal("test", chunk.ExtractCell(1).SafeToString());
        chunk.Next();
        Assert.Equal("value", chunk.ExtractCell(0).SafeToString());
        Assert.Null(chunk.ExtractCell(1).SafeToString());
    }

    private static SFReusableChunk CreateChunk(int colCount, int rowCount)
    {
        var chunkInfo = new ExecResponseChunk
        {
            url = "fake",
            uncompressedSize = 1024,
            rowCount = rowCount
        };
        var chunk = new SFReusableChunk(colCount);
        chunk.Reset(chunkInfo, 0);
        return chunk;
    }

    private static string[] GenerateRows(int count)
    {
        var rows = new string[count];
        for (var i = 0; i < count; i++)
            rows[i] = "[\"value\"]";
        return rows;
    }
}
