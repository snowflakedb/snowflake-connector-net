/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class ArrowResultSet : SFBaseResultSet
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ArrowResultSet>();
        
        private readonly int _totalChunkCount;
        private BaseResultChunk _currentChunk;
        private readonly IChunkDownloader _chunkDownloader;

        public ArrowResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            columnCount = responseData.rowType.Count;
            try
            {
                using (var stream = new MemoryStream(Convert.FromBase64String(responseData.rowsetBase64)))
                {
                    using (var reader = new ArrowStreamReader(stream))
                    {
                        var recordBatch = reader.ReadNextRecordBatch();
                        _currentChunk = new ArrowResultChunk(recordBatch);
                    }
                }

                this.sfStatement = sfStatement;
                UpdateSessionStatus(responseData);

                if (responseData.chunks != null)
                {
                    _totalChunkCount = responseData.chunks.Count;
                    _chunkDownloader = ChunkDownloaderFactory.GetDownloader(responseData, this, cancellationToken);
                }

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

        internal override async Task<bool> NextAsync()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_totalChunkCount > 0)
            {
                s_logger.Debug("Get next chunk from chunk downloader");
                _currentChunk = await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false);
                return _currentChunk?.Next() ?? false;
            }

            return false;
        }

        internal override bool Next()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;
            
            if (_totalChunkCount > 0)
            {
                s_logger.Debug("Get next chunk from chunk downloader");
                _currentChunk = Task.Run(async() => await (_chunkDownloader.GetNextChunkAsync()).ConfigureAwait(false)).Result;
                return _currentChunk?.Next() ?? false;
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

            return _currentChunk.RowCount > 0 || _totalChunkCount > 0;
        }

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            ThrowIfClosed();

            if (_currentChunk.Rewind())
                return true;

            if (_currentChunk.ChunkIndex > 0)
            {
                s_logger.Warn("Unable to rewind to the previous chunk");
            }

            return false;
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
        }
    }
}
