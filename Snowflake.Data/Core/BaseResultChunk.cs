/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core
{
    public enum ResultFormat
    {
        JSON,
        ARROW
    }
    
    public abstract class BaseResultChunk
    {
        internal abstract ResultFormat Format { get; }
        
        public int RowCount { get; protected set; }
        
        public int ColumnCount { get; protected set; }
        
        public int ChunkIndex { get; protected set; }

        internal string Url { get; set; }

        internal string[,] RowSet { get; set; }

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
