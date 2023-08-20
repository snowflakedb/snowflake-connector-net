/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Snowflake.Data.Core
{
    class SFResultSetArrow : SFBaseResultSet
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFResultSetArrow>();
        
        private int _currentChunkRowIdx;
        private int _currentChunkRowCount;
        private readonly int _totalChunkCount;
        private IResultChunk _currentChunk;
        private readonly IChunkDownloader _chunkDownloader;

        public SFResultSetArrow(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            try
            {
                columnCount = responseData.rowType.Count;
                _currentChunkRowIdx = -1;

                using (var stream = new MemoryStream(Convert.FromBase64String(responseData.rowsetBase64)))
                {
                    using (var reader = new ArrowStreamReader(stream))
                    {
                        var recordBatch = reader.ReadNextRecordBatch();
                        _currentChunkRowCount = recordBatch.Length;
                        _currentChunk = new SFArrowResultChunk(recordBatch);
                    }
                }

                this.sfStatement = sfStatement;
                updateSessionStatus(responseData);

                if (responseData.chunks != null)
                {
                    // TODO - support for multiple chunks
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }

                responseData.rowSet = null;

                sfResultSetMetaData = new SFResultSetMetaData(responseData);

                isClosed = false;

                queryId = responseData.queryId;
            }
            catch(System.Exception ex)
            {
                s_logger.Error("Result set error queryId="+responseData.queryId, ex);
                throw;
            }
        }

        internal override async Task<bool> NextAsync()
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

            if (_chunkDownloader != null)
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            return false;
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

            if (_chunkDownloader != null)
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            return false;
        }

        internal override bool NextResult()
        {
            return false;
        }

        internal override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return await Task.FromResult(false);
        }

        internal override bool HasRows()
        {
            if (isClosed)
            {
                return false;
            }

            return _currentChunkRowCount > 0 || _totalChunkCount > 0;
        }

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            if (_currentChunkRowIdx >= 0)
            {
                _currentChunkRowIdx--;
                if (_currentChunkRowIdx >= _currentChunkRowCount)
                {
                    return true;
                }
            }

            return false;
        }

        internal override UTF8Buffer getObjectInternal(int columnIndex)
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            if (columnIndex < 0 || columnIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, columnIndex);
            }

            return _currentChunk.ExtractCell(_currentChunkRowIdx, columnIndex);
        }

        private void updateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.database = responseData.finalDatabaseName;
            session.schema = responseData.finalSchemaName;

            session.UpdateSessionParameterMap(responseData.parameters);
        }
    }
}
