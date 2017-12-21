/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using Common.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        private static readonly ILog logger = LogManager.GetLogger<SFResultSet>();

        private QueryExecResponseData execFirstChunkData;

        private int currentChunkRowIdx;

        private int currentChunkRowCount;

        private int totalChunkCount;

        private int currentChunkIndex;

        private SFChunkDownloader chunkDownloader;

        private SFResultChunk currentChunk;

        public SFResultSet(QueryExecResponseData responseData, SFStatement sfStatement)
        {
            execFirstChunkData = responseData;
            columnCount = responseData.rowType.Count;
            currentChunkRowIdx = -1;
            currentChunkRowCount = responseData.rowSet.GetLength(0);
            currentChunkIndex = 0;

            this.sfStatement = sfStatement;

            if (responseData.chunks != null)
            {
                // counting the first chunk
                totalChunkCount = responseData.chunks.Count + 1;
                chunkDownloader = new SFChunkDownloader(responseData.rowType.Count,
                                                        responseData.chunks,
                                                        responseData.qrmk,
                                                        responseData.chunkHeaders);
            }

            currentChunk = new SFResultChunk(responseData.rowSet);

            sfResultSetMetaData = new SFResultSetMetaData(responseData);

            updateSessionStatus(responseData);
            isClosed = false;
        }

        internal override bool next()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            currentChunkRowIdx++;
            
            if (currentChunkRowIdx < currentChunkRowCount)
            {
                return true;
            }
            else if (++currentChunkIndex < totalChunkCount)
            {
                execFirstChunkData.rowSet = null;
                logger.DebugFormat("Get chunk #{0} to consume", currentChunkIndex);

                SFResultChunk chunk = chunkDownloader.getNextChunkToConsume();

                if (chunk == null)
                {
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Failed to get chunk to consume");
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

        protected override string getObjectInternal(int columnIndex)
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            if (columnIndex < 0 || columnIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, columnIndex);
            }

            return currentChunk.extractCell(currentChunkRowIdx, columnIndex);
        }

        private void updateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.sfSession;
            session.database = responseData.finalDatabaseName;
            session.schema = responseData.finalSchemaName;

            SFSession.updateParameterMap(session.parameterMap, responseData.parameters);
        }
    }
}
