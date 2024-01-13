/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core
{
    public abstract class BaseResultChunk : IResultChunk
    {
        internal abstract ResultFormat ResultFormat { get; }
        
        public int RowCount { get; protected set; }
        
        public int ColumnCount { get; protected set; }
        
        public int ChunkIndex { get; protected set; }

        internal int CompressedSize;
        
        internal int UncompressedSize;

        internal string Url { get; set; }

        internal string[,] RowSet { get; set; }
        
        public int GetRowCount() => RowCount;

        public int GetChunkIndex() => ChunkIndex;

        [Obsolete("ExtractCell with rowIndex is deprecated", false)]
        public abstract UTF8Buffer ExtractCell(int rowIndex, int columnIndex);

        public abstract UTF8Buffer ExtractCell(int columnIndex);
        
        internal abstract bool Next();
        
        internal abstract bool Rewind();
        
        internal virtual void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            RowCount = chunkInfo.rowCount;
            Url = chunkInfo.url;
            ChunkIndex = chunkIndex;
            CompressedSize = chunkInfo.compressedSize;
            UncompressedSize = chunkInfo.uncompressedSize;
        }

        internal virtual void ResetForRetry()
        {
        }
    }
}
