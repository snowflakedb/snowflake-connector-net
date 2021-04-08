/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System;
using System.Diagnostics;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFResultSet>();
        
        private int _currentChunkRowIdx;

        private int _currentChunkRowCount;

        private readonly int _totalChunkCount;
        
        private readonly IChunkDownloader _chunkDownloader;

        private IResultChunk _currentChunk;

        private Stopwatch stopwatch = Stopwatch.StartNew();

        public SFResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            columnCount = responseData.rowType.Count;
            _currentChunkRowIdx = -1;
            _currentChunkRowCount = responseData.rowSet.GetLength(0);
           
            this.sfStatement = sfStatement;
            updateSessionStatus(responseData);

            if (responseData.chunks != null)
            {
                // counting the first chunk
                _totalChunkCount = responseData.chunks.Count;
                _chunkDownloader = ChunkDownloaderFactory.GetDownloader(responseData, this, cancellationToken);
            }

            _currentChunk = new SFResultChunk(responseData.rowSet);
            responseData.rowSet = null;

            sfResultSetMetaData = new SFResultSetMetaData(responseData);

            isClosed = false;
        }

        internal void resetChunkInfo(IResultChunk nextChunk)
        {
            Logger.Debug($"Recieved chunk #{nextChunk.GetChunkIndex() + 1} of {_totalChunkCount}");
            if (_currentChunk is SFResultChunk)
            {
                ((SFResultChunk)_currentChunk).rowSet = null;
            }
            _currentChunk = nextChunk;
            _currentChunkRowIdx = 0;
            _currentChunkRowCount = _currentChunk.GetRowCount();
        }

        internal override async Task<bool> NextAsync()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            if (0 == fetchCount)
            {
                Logger.Info("Starting. Fetching first row.");
                stopwatch.Restart();
            }
            else if (0 == (fetchCount % 100000))
            {
                LogAverageConversionTimesForLast100000rows();
            }

            fetchCount++;
            _currentChunkRowIdx++;
            if (_currentChunkRowIdx < _currentChunkRowCount)
            {
                if (0 == (fetchCount %100000))
                {
                    Logger.Info($"Fetched {fetchCount} rows");
                }
                return true;
            }

            if (_chunkDownloader != null)
            {
                // GetNextChunk could be blocked if download result is not done yet. 
                // So put this piece of code in a seperate task
                Logger.Info("Get next chunk from chunk downloader");
                IResultChunk nextChunk = await
                    Measure(
                        @"nextChunk",
                        out double nextChunkTime,
                        async () =>
                        {
                            IResultChunk result = await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false);
                            return result;
                        });
                if (nextChunk != null)
                {
                    resetChunkInfo(nextChunk);
                    if (0 == (fetchCount % 100000))
                    {
                        Logger.Info($"Fetched {fetchCount} rows");
                    }
                    return true;
                }
                else
                {
                    stopwatch.Stop();
                    Logger.Info("All data retrieved. Last row returned.");
                    Logger.Info($"Fetch process took {stopwatch.Elapsed.TotalMilliseconds} ms");
                    if (timings.TryGetValue("nextChunk", out double nextChunkTotalTime))
                    {
                        Logger.Debug("---- Time spent getting next chunk ----");
                        Logger.Debug($"Total time : {nextChunkTotalTime} ms");
                        Logger.Debug($"Average time : {nextChunkTotalTime / (float)_totalChunkCount } ms\n");
                    }
                    LogAverageConversionTimes();
                    return false;
                }
            }

            stopwatch.Stop();
            Logger.Info("All data retrieved. Last row returned.");
            Logger.Info($"Fetch process took {stopwatch.Elapsed.TotalMilliseconds} ms");
            if (Logger.IsDebugEnabled())
            {
                if (timings.TryGetValue("nextChunk", out double nextChunkTotalTime))
                {
                    Logger.Debug("---- Time spent getting next chunk ----");
                    Logger.Debug($"Total time : {nextChunkTotalTime} ms");
                    Logger.Debug($"Average time : {nextChunkTotalTime / (float)_totalChunkCount } ms\n");
                }
                LogAverageConversionTimes();
            }
            return false;
        }

        internal override bool Next()
        {
            if (isClosed)
            {
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
            }

            if (0 == fetchCount)
            {
                Logger.Info("Starting. Fetching first row.");
                stopwatch.Restart();
            }
            else if (0 == (fetchCount % 100000))
            {
                LogAverageConversionTimesForLast100000rows();
            }

            fetchCount++;
            _currentChunkRowIdx++;
            if (_currentChunkRowIdx < _currentChunkRowCount)
            {
                if (0 == (fetchCount % 100000))
                {
                    Logger.Info($"Fetched {fetchCount} rows");
                }
                return true;
            }

            if (_chunkDownloader != null)
            {
                Logger.Info("Get next chunk from chunk downloader");
                IResultChunk nextChunk = Measure(
                        @"nextChunk",
                        out double nextChunkTime,
                        () =>
                        {
                            return Task.Run(async () => await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false)).Result;
                        });

                if (nextChunk != null)
                {
                    resetChunkInfo(nextChunk);
                    if (0 == (fetchCount % 100000))
                    {
                        Logger.Info($"Fetched {fetchCount} rows");
                    }
                    return true;
                }
            }

            stopwatch.Stop();
            Logger.Info("All data retrieved. Last row returned.");
            Logger.Info($"Fetch process took {stopwatch.Elapsed.TotalMilliseconds} ms");
            if (Logger.IsDebugEnabled())
            {
                if (timings.TryGetValue("nextChunk", out double nextChunkTotalTime))
                {
                    Logger.Debug("---- Time spent getting next chunk ----");
                    Logger.Debug($"Total time : {nextChunkTotalTime} ms");
                    Logger.Debug($"Average time : {nextChunkTotalTime / (float)_totalChunkCount } ms\n");
                }
                LogAverageConversionTimes();
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
