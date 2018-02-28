/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core
{
    internal class SFResultChunk
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

        public string extractCell(int rowIndex, int columnIndex)
        {
            return rowSet[rowIndex, columnIndex];
        }

        public void addValue(string val, int rowCount, int colCount)
        {
            rowSet[rowCount, colCount] = val;
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
