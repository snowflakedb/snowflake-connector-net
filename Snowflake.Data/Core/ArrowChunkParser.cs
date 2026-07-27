using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Snowflake.Data.Core;

internal sealed class ArrowChunkParser : IChunkParser
{
    private readonly Stream _stream;

    internal ArrowChunkParser(Stream stream)
    {
        _stream = stream;
    }

    public async Task ParseChunkAsync(IResultChunk chunk, CancellationToken cancellationToken)
    {
        var resultChunk = (ArrowResultChunk)chunk;

        using var reader = new ArrowStreamReader(_stream);
        while (await reader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false) is { } recordBatch)
        {
            resultChunk.AddRecordBatch(recordBatch);
        }
    }
}
