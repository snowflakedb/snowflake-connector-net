/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Text;

namespace Snowflake.Data.Core
{
    internal class SFResultChunk : IResultChunk
    {
        public string[,] rowSet { get; set; }

        public int rowCount { get; set; }

        public int colCount { get; set; }

        public string url { get; set; }

        public DownloadState downloadState { get; set; }
        public int ChunkIndex { get;  }

        public readonly object syncPrimitive; 

        public SFResultChunk(string[,] rowSet)
        {
            this.rowSet = rowSet;
            this.rowCount = rowSet.GetLength(0);
            this.downloadState = DownloadState.NOT_STARTED;
        }

        public SFResultChunk(string url, int rowCount, int colCount, int index)
        {
            this.rowCount = rowCount;
            this.colCount = colCount;
            this.url = url;
            ChunkIndex = index;
            syncPrimitive = new object();
            this.downloadState = DownloadState.NOT_STARTED;
        }

        public UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            // Convert string to UTF8Buffer. This makes this method a little slower, but this class is not used for large result sets
            string s = rowSet[rowIndex, columnIndex];
            if (s == null)
                return null;
            byte[] b = Encoding.UTF8.GetBytes(s);
            return new UTF8Buffer(b);
        }

        public void addValue(string val, int rowCount, int colCount)
        {
            rowSet[rowCount, colCount] = val;
        }

        public int GetRowCount()
        {
            return rowCount;
        }

        public int GetChunkIndex()
        {
            return ChunkIndex;
        }
    }

    public enum DownloadState
    {
        NOT_STARTED,
        IN_PROGRESS,
        SUCCESS,
        FAILURE
    }
}
