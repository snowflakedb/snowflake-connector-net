using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        private QueryExecResponseData execFirstChunkData;

        private int currentChunkRowIdx;

        private int currentChunkRowCount;

        private int totalChunkCount;

        private int currentChunkIndex;

        private SFChunkDownloader chunkDownloader;

        private SFResultChunk currentChunk;

        public SFResultSet(QueryExecResponseData responseData)
        {
            execFirstChunkData = responseData;
            currentChunkRowIdx = -1;
            currentChunkRowCount = responseData.rowSet.GetLength(0);
            currentChunkIndex = 0;

            if (responseData.chunks != null)
            {
                // counting the first chunk
                totalChunkCount = responseData.chunks.Count + 1;
                chunkDownloader = new SFChunkDownloader(responseData.rowType.Count,
                                                        responseData.chunks,
                                                        responseData.qrmk);
            }

            currentChunk = new SFResultChunk(responseData.rowSet);
        }

        public override bool next()
        {
            currentChunkRowIdx++;
            
            if (currentChunkRowIdx < currentChunkRowCount)
            {
                return true;
            }
            else if (++currentChunkIndex < totalChunkCount)
            {
                execFirstChunkData.rowSet = null; 
                SFResultChunk chunk = chunkDownloader.getNextChunkToConsume();

                if (chunk == null)
                {
                    throw new SFException();
                }
                currentChunk = chunk;
                currentChunkRowIdx = 0;
                currentChunkRowCount = currentChunk.rowCount;

                return true;                     
            }
            else
            {
                return false;
            }
        }

        protected override object getObjectInternal(int columnIndex)
        {
            return currentChunk.extractCell(currentChunkRowIdx, columnIndex);
        }
    }
}
