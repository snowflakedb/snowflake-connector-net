using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal class ArrowChunkParser : IChunkParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ArrowChunkParser>();

        private readonly Stream stream;

        internal ArrowChunkParser(Stream stream)
        {
            this.stream = stream;
        }

        public async Task ParseChunk(IResultChunk chunk)
        {
            ArrowResultChunk resultChunk = (ArrowResultChunk)chunk;

            var streamInfo = "N/A";
            try { streamInfo = $"len={stream.Length}, pos={stream.Position}"; } catch { }
            s_logger.Info($"[ArrowBatchTrace] ParseChunk: starting chunk={resultChunk.ChunkIndex}, stream={streamInfo}");

            int batchIdx = 0;
            using (var reader = new ArrowStreamReader(stream))
            {
                RecordBatch recordBatch;
                while ((recordBatch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                {
                    s_logger.Info($"[ArrowBatchTrace] ParseChunk: chunk={resultChunk.ChunkIndex} " +
                                   $"batch[{batchIdx}]: rows={recordBatch.Length}, cols={recordBatch.ColumnCount}");
                    if (recordBatch.ColumnCount != resultChunk.ColumnCount && batchIdx > 0)
                    {
                        s_logger.Error($"[ArrowBatchTrace] ParseChunk: COLUMN MISMATCH in chunk={resultChunk.ChunkIndex} " +
                                       $"batch[{batchIdx}]: has {recordBatch.ColumnCount} cols, " +
                                       $"expected {resultChunk.ColumnCount}");
                    }
                    resultChunk.AddRecordBatch(recordBatch);
                    batchIdx++;
                }
            }
            s_logger.Info($"[ArrowBatchTrace] ParseChunk: chunk={resultChunk.ChunkIndex} complete: " +
                           $"{batchIdx} batches, totalRows={resultChunk.RowCount}, " +
                           $"cols={resultChunk.ColumnCount}");
        }
    }
}
