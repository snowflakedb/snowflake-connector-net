using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Snowflake.Data.Core
{
    public class ArrowChunkParser : IChunkParser
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
