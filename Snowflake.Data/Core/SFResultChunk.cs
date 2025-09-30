using System;
using System.Text;

namespace Snowflake.Data.Core
{
    internal class SFResultChunk : BaseResultChunk
    {
        internal override ResultFormat ResultFormat => ResultFormat.JSON;

        private int _currentRowIndex = -1;

        public SFResultChunk(string[,] rowSet)
        {
            RowSet = rowSet;
            RowCount = rowSet.GetLength(0);
            ColumnCount = rowSet.GetLength(1);
            ChunkIndex = -1;
        }

        public SFResultChunk(string url, int rowCount, int columnCount, int index)
        {
            RowCount = rowCount;
            ColumnCount = columnCount;
            Url = url;
            ChunkIndex = index;
        }

        [Obsolete("ExtractCell with rowIndex is deprecated", false)]
        public override UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            _currentRowIndex = rowIndex;
            return ExtractCell(columnIndex);
        }

        public override UTF8Buffer ExtractCell(int columnIndex)
        {
            // Convert string to UTF8Buffer. This makes this method a little slower, but this class is not used for large result sets
            string s = RowSet[_currentRowIndex, columnIndex];
            if (s == null)
                return null;
            byte[] b = Encoding.UTF8.GetBytes(s);
            return new UTF8Buffer(b);
        }

        internal override bool Next()
        {
            _currentRowIndex += 1;
            return _currentRowIndex < RowCount;
        }

        internal override bool Rewind()
        {
            _currentRowIndex -= 1;
            return _currentRowIndex >= 0;
        }

        internal override void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            base.Reset(chunkInfo, chunkIndex);
            _currentRowIndex = -1;
        }
    }
}
