/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        private static readonly ILog Logger = LogManager.GetLogger<SFResultSet>();
        
        private int _currentChunkRowIdx;

        private int _currentChunkRowCount;

        private readonly int _totalChunkCount;
        
        private readonly SFChunkDownloader _chunkDownloader;

        private SFResultChunk _currentChunk;

        public SFResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken)
        {
            columnCount = responseData.rowType.Count;
            _currentChunkRowIdx = -1;
            _currentChunkRowCount = responseData.rowSet.GetLength(0);
           
            this.sfStatement = sfStatement;

            if (responseData.chunks != null)
            {
                // counting the first chunk
                _totalChunkCount = responseData.chunks.Count + 1;
                _chunkDownloader = new SFChunkDownloader(columnCount,
                                                        responseData.chunks,
                                                        responseData.qrmk,
                                                        responseData.chunkHeaders,
                                                        cancellationToken);
            }

            _currentChunk = new SFResultChunk(responseData.rowSet);
            responseData.rowSet = null;

            sfResultSetMetaData = new SFResultSetMetaData(responseData);

            updateSessionStatus(responseData);
            isClosed = false;
        }

        internal void resetChunkInfo(SFResultChunk nextChunk)
        {
            Logger.DebugFormat("Recieved chunk #{0} of {1}", nextChunk.ChunkIndex+1, _totalChunkCount);
            _currentChunk.rowSet = null;
            _currentChunk = nextChunk;
            _currentChunkRowIdx = 0;
            _currentChunkRowCount = _currentChunk.rowCount;
        }

        internal override Task<bool> NextAsync()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            _currentChunkRowIdx++;
            if (_currentChunkRowIdx < _currentChunkRowCount)
            {
                return Task.FromResult(true);
            }

            if (_chunkDownloader != null)
            {
                // GetNextChunk could be blocked if download result is not done yet. 
                // So put this piece of code in a seperate task
                return Task.Run(() =>
                {
                    SFResultChunk nextChunk;
                    if ((nextChunk = _chunkDownloader.GetNextChunk()) != null)
                    {
                        resetChunkInfo(nextChunk);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                });
            }
            
           return Task.FromResult(false);
        }

        internal override bool Next()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            _currentChunkRowIdx++;
            if (_currentChunkRowIdx < _currentChunkRowCount)
            {
                return true;
            }

            SFResultChunk nextChunk;
            if ((nextChunk = _chunkDownloader?.GetNextChunk()) != null)
            {
                resetChunkInfo(nextChunk);
                return true;
            }
            
           return false;
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

            return _currentChunk.extractCell(_currentChunkRowIdx, columnIndex);
        }

        private void updateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.database = responseData.finalDatabaseName;
            session.schema = responseData.finalSchemaName;

            SFSession.updateParameterMap(session.parameterMap, responseData.parameters);
        }
    }
}
