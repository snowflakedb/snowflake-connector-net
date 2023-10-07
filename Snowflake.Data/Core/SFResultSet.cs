/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFResultSet>();
        
        private readonly int _totalChunkCount;
        
        private readonly IChunkDownloader _chunkDownloader;

        private BaseResultChunk _currentChunk;

        public SFResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            try
            {
                columnCount = responseData.rowType.Count;

                this.sfStatement = sfStatement;
                UpdateSessionStatus(responseData);

                if (responseData.chunks != null)
                {
                    // counting the first chunk
                    _totalChunkCount = responseData.chunks.Count;
                    _chunkDownloader = ChunkDownloaderFactory.GetDownloader(responseData, this, cancellationToken);
                }

                _currentChunk = new SFResultChunk(responseData.rowSet);
                responseData.rowSet = null;

                sfResultSetMetaData = new SFResultSetMetaData(responseData, this.sfStatement.SfSession);

                isClosed = false;

                queryId = responseData.queryId;
            }
            catch(System.Exception ex)
            {
                s_logger.Error("Result set error queryId="+responseData.queryId, ex);
                throw;
            }
        }

        public enum PutGetResponseRowTypeInfo {   
            SourceFileName                    = 0,
            DestinationFileName               = 1,
            SourceFileSize                    = 2,
            DestinationFileSize               = 3,
            SourceCompressionType             = 4,
            DestinationCompressionType        = 5,
            ResultStatus                      = 6,
            ErrorDetails                      = 7
            }

        public void InitializePutGetRowType(List<ExecResponseRowType> rowType)
        {
            foreach (PutGetResponseRowTypeInfo t in System.Enum.GetValues(typeof(PutGetResponseRowTypeInfo)))
            {
                rowType.Add(new ExecResponseRowType()
                {
                    name = t.ToString(),
                    type = "text"
                });
            }
        }

        public SFResultSet(PutGetResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            responseData.rowType = new List<ExecResponseRowType>();
            InitializePutGetRowType(responseData.rowType);

            columnCount = responseData.rowType.Count;

            this.sfStatement = sfStatement;

            _currentChunk = new SFResultChunk(responseData.rowSet);
            responseData.rowSet = null;

            sfResultSetMetaData = new SFResultSetMetaData(responseData);

            isClosed = false;

            queryId = responseData.queryId;
        }

        internal void ResetChunkInfo(BaseResultChunk nextChunk)
        {
            s_logger.Debug($"Received chunk #{nextChunk.ChunkIndex + 1} of {_totalChunkCount}"); 
            _currentChunk.RowSet = null;
            _currentChunk = nextChunk;
        }

        internal override async Task<bool> NextAsync()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_chunkDownloader != null)
            {
                // GetNextChunk could be blocked if download result is not done yet. 
                // So put this piece of code in a seperate task
                s_logger.Debug("Get next chunk from chunk downloader");
                BaseResultChunk nextChunk = await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false);
                if (nextChunk != null)
                {
                    ResetChunkInfo(nextChunk);
                    return _currentChunk.Next();
                }
            }
            
            return false;
        }

        internal override bool Next()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_chunkDownloader != null)
            {
                s_logger.Debug("Get next chunk from chunk downloader");
                BaseResultChunk nextChunk = Task.Run(async() => await (_chunkDownloader.GetNextChunkAsync()).ConfigureAwait(false)).Result;
                if (nextChunk != null)
                {
                    ResetChunkInfo(nextChunk);
                    return _currentChunk.Next();
                }
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
            ThrowIfClosed();

            return _currentChunk.RowCount > 0 || _totalChunkCount > 0;
        }

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            ThrowIfClosed();

            return _currentChunk.Rewind();
        }

        internal override UTF8Buffer getObjectInternal(int columnIndex)
        {
            ThrowIfClosed();

            if (columnIndex < 0 || columnIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, columnIndex);
            }

            return _currentChunk.ExtractCell(columnIndex);
        }

        private void UpdateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.UpdateDatabaseAndSchema(responseData.finalDatabaseName, responseData.finalSchemaName);
            session.UpdateSessionParameterMap(responseData.parameters);
            session.UpdateQueryContextCache(responseData.QueryContext);
        }
    }
}
