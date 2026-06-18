using System;

namespace Snowflake.Data.Core
{
    internal abstract class BaseResultChunk : IResultChunk
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

        internal virtual void Clear()
        {
            RowCount = 0;
            Url = null;
            ChunkIndex = 0;
            CompressedSize = 0;
            UncompressedSize = 0;
        }

        internal virtual void ResetForRetry()
        {
        }
    }
}
