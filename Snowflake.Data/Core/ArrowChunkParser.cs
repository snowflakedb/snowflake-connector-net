using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Snowflake.Data.Core
{
    internal class ArrowChunkParser : IChunkParser
    {
        private readonly Stream stream;

        internal ArrowChunkParser(Stream stream)
        {
            this.stream = stream;
        }

        public async Task ParseChunk(IResultChunk chunk)
        {
            ArrowResultChunk resultChunk = (ArrowResultChunk)chunk;

            using (var reader = new ArrowStreamReader(stream))
            {
                RecordBatch recordBatch;
                while ((recordBatch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                {
                    resultChunk.AddRecordBatch(recordBatch);
                }
            }
        }
    }
}
