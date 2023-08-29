/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

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

        public async Task ParseChunk(BaseResultChunk chunk)
        {
            ArrowResultChunk rc = (ArrowResultChunk)chunk; 

            using (var reader = new ArrowStreamReader(stream))
            {
                var recordBatch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
                while (recordBatch != null)
                {
                    rc.AddRecordBatch(recordBatch);
                    recordBatch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
                }
            }
        }
    }
}
