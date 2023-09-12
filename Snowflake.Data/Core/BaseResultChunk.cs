/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core
{
    public abstract class BaseResultChunk : IResultChunk
    {
        internal abstract ResultFormat Format { get; }
        
        public int RowCount { get; protected set; }
        
        public int ColumnCount { get; protected set; }
        
        public int ChunkIndex { get; protected set; }

        internal string Url { get; set; }

        internal string[,] RowSet { get; set; }
        
        public int GetRowCount() => RowCount;

        public int GetChunkIndex() => ChunkIndex;

        public abstract UTF8Buffer ExtractCell(int rowIndex, int columnIndex);

        public abstract UTF8Buffer ExtractCell(int columnIndex);
        
        internal abstract bool Next();
        
        internal abstract bool Rewind();

        internal virtual void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            RowCount = chunkInfo.rowCount;
            Url = chunkInfo.url;
            ChunkIndex = chunkIndex;
        }

        internal virtual void ResetForRetry()
        {
        }
    }
}
