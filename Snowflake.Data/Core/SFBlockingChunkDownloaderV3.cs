/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO.Compression;
using System.IO;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using System.Diagnostics;
using Newtonsoft.Json.Serialization;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    using Client;
    using Session;
    using Tools;

    class SFBlockingChunkDownloaderV3 : IChunkDownloader, IDisposable
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();

        private List<BaseResultChunk> chunkDatas = new List<BaseResultChunk>();

        private string qrmk;

        private int nextChunkToDownloadIndex;

        private int nextChunkToConsumeIndex;

        // External cancellation token, used to stop donwload
        private CancellationToken externalCancellationToken;

        private int prefetchSlot;

        private readonly IRestRequester _RestRequester;

        private readonly SFSessionProperties sessionProperies;

        private Dictionary<string, string> chunkHeaders;

        private readonly SFBaseResultSet ResultSet;

        private readonly List<ExecResponseChunk> chunkInfos;

        private readonly List<Task<BaseResultChunk>> taskQueues;
        private readonly long chunkBlockSize;

        public SFBlockingChunkDownloaderV3(int colCount,
            List<ExecResponseChunk> chunkInfos, string qrmk,
            Dictionary<string, string> chunkHeaders,
            CancellationToken cancellationToken,
            SFBaseResultSet ResultSet,
            ResultFormat resultFormat)
        {
            this.qrmk = qrmk;
            this.chunkHeaders = chunkHeaders;
            this.nextChunkToDownloadIndex = 0;
            this.ResultSet = ResultSet;
            this._RestRequester = ResultSet.sfStatement.SfSession.restRequester;
            this.sessionProperies = ResultSet.sfStatement.SfSession.properties;
            this.prefetchSlot = Math.Min(chunkInfos.Count, GetPrefetchThreads(ResultSet));
            this.chunkInfos = chunkInfos;
            this.chunkBlockSize = this.calculateChunkBlockSize(this.chunkInfos);
            this.nextChunkToConsumeIndex = 0;
            this.taskQueues = new List<Task<BaseResultChunk>>();
            externalCancellationToken = cancellationToken;

            if (chunkInfos.Any(c => c.uncompressedSize == 0))
            {
                logger.Debug("DEBUG: Found chunk with no data, skipping download");
                int i = 0;
                chunkInfos.ForEach(c =>
                {
                    logger.Debug($"DEBUG: Chunk # {i} info: {JsonConvert.SerializeObject(c)}");
                    i++;
                });
            }

            EnsureMemorySpaceForChunkDownloader();

            for (int i=0; i<prefetchSlot; i++)
            {
                BaseResultChunk resultChunk =
                    resultFormat == ResultFormat.ARROW ? (BaseResultChunk)
                        new ArrowResultChunk(colCount) :
                        new SFReusableChunk(colCount, chunkBlockSize);

                resultChunk.Reset(chunkInfos[nextChunkToDownloadIndex], nextChunkToDownloadIndex);
                chunkDatas.Add(resultChunk);

                taskQueues.Add(DownloadChunkAsync(new DownloadContextV3()
                {
                    chunk = resultChunk,
                    qrmk = this.qrmk,
                    chunkHeaders = this.chunkHeaders,
                    cancellationToken = this.externalCancellationToken
                }));

                nextChunkToDownloadIndex++;
            }
        }

        private void EnsureMemorySpaceForChunkDownloader()
        {
            var hasMemory = true;
            var chunkAvailableSlots = this.prefetchSlot;
            logger.Info($"Requesting memory for chunk downloader: {chunkAvailableSlots}, available memory: {SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable()}");

            while(chunkAvailableSlots >= 2)
            {
                var memoryAvailable = SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable();
                var initialMemory = SFChunkMemoryManager.Instance.GetInitialMemoryAvailable();
                if (memoryAvailable < initialMemory * .3 && chunkAvailableSlots > 2)
                {
                    chunkAvailableSlots = 2;
                }else if (memoryAvailable < initialMemory * .5 && chunkAvailableSlots > 2)
                {
                    chunkAvailableSlots = (int)(Math.Max(2, chunkAvailableSlots / 2));
                }

                if(SFChunkMemoryManager.Instance.TryRetainMemory((int)chunkBlockSize * chunkAvailableSlots))
                {
                    logger.Info($"Retained chunks for chunk downloader: {chunkAvailableSlots}, available memory: {SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable()}");
                    this.prefetchSlot = chunkAvailableSlots;
                    return;
                }
                chunkAvailableSlots--;
            }

            chunkAvailableSlots = 2;
            logger.Info($"Waiting for chunks to be available: {chunkAvailableSlots}, available memory: {SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable()}");

            hasMemory &= SFChunkMemoryManager.Instance.WaitForMemoryAvailable(chunkBlockSize * chunkAvailableSlots);
            if (!hasMemory)
            {
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Not enough memory available for chunk downloader");
            }
            else
            {
                logger.Info($"Obtained chunks after wait for: {chunkAvailableSlots}, available memory: {SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable()}");

            }

            this.prefetchSlot = chunkAvailableSlots;

        }



        private long calculateChunkBlockSize(List<ExecResponseChunk> execResponseChunks)
        {
            // Assuming execResponseChunks is a collection of objects with a property uncompressedSize
            long largestUncompressedChunk = execResponseChunks.Max(chunk => chunk.uncompressedSize);
            var chunkBlockSize = Math.Min(largestUncompressedChunk, 1 << 24);
            return 1 << 24;
        }

        private int GetPrefetchThreads(SFBaseResultSet resultSet)
        {
            Dictionary<SFSessionParameter, object> sessionParameters = resultSet.sfStatement.SfSession.ParameterMap;
            String val = (String)sessionParameters[SFSessionParameter.CLIENT_PREFETCH_THREADS];
            return Int32.Parse(val);
        }

        public async Task<BaseResultChunk> GetNextChunkAsync()
        {
            logger.Info($"NextChunkToConsume: {nextChunkToConsumeIndex}, NextChunkToDownload: {nextChunkToDownloadIndex}");
            if (nextChunkToConsumeIndex < chunkInfos.Count)
            {
                Task<BaseResultChunk> chunk = taskQueues[nextChunkToConsumeIndex % prefetchSlot];

                if (nextChunkToDownloadIndex < chunkInfos.Count && nextChunkToConsumeIndex > 0)
                {
                    BaseResultChunk reusableChunk = chunkDatas[nextChunkToDownloadIndex % prefetchSlot];
                    reusableChunk.Reset(chunkInfos[nextChunkToDownloadIndex], nextChunkToDownloadIndex);

                    taskQueues[nextChunkToDownloadIndex % prefetchSlot] = DownloadChunkAsync(new DownloadContextV3()
                    {
                        chunk = reusableChunk,
                        qrmk = this.qrmk,
                        chunkHeaders = this.chunkHeaders,
                        cancellationToken = externalCancellationToken
                    });
                    nextChunkToDownloadIndex++;

                    // in case of one slot we need to return the chunk already downloaded
                    if (prefetchSlot == 1)
                    {
                        chunk = taskQueues[0];
                    }
                }
                nextChunkToConsumeIndex++;
                return await chunk;
            }
            else
            {
                return await Task.FromResult<BaseResultChunk>(null);
            }
        }

        private async Task<BaseResultChunk> DownloadChunkAsync(DownloadContextV3 downloadContext)
        {
            BaseResultChunk chunk = downloadContext.chunk;
            int backOffInSec = 1;
            bool retry = false;
            int retryCount = 0;
            int maxRetry = int.Parse(sessionProperies[SFSessionProperty.MAXHTTPRETRIES]);

            do
            {
                retry = false;

                if(chunk.UncompressedSize == 0) {
                    logger.Info($"Chunk {chunk.ChunkIndex} has no data, skipping download");
                    return chunk;
                }

                S3DownloadRequest downloadRequest =
                    new S3DownloadRequest()
                    {
                        Url = new UriBuilder(chunk.Url).Uri,
                        qrmk = downloadContext.qrmk,
                        // s3 download request timeout to one hour
                        RestTimeout = TimeSpan.FromHours(1),
                        HttpTimeout = Timeout.InfiniteTimeSpan, // Disable timeout for each request
                        chunkHeaders = downloadContext.chunkHeaders,
                        sid = ResultSet.sfStatement.SfSession.sessionId
                    };

                using (var httpResponse = await _RestRequester.GetAsync(downloadRequest, downloadContext.cancellationToken)
                               .ConfigureAwait(continueOnCapturedContext: false))
                using (Stream stream = await httpResponse.Content.ReadAsStreamAsync()
                    .ConfigureAwait(continueOnCapturedContext: false))
                {
                    // retry on chunk downloading since the retry logic in HttpClient.RetryHandler
                    // doesn't cover this. The GET request could be succeeded but network error
                    // still could happen during reading chunk data from stream and that needs
                    // retry as well.
                    try
                    {
                        IEnumerable<string> encoding;
                        if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out encoding))
                        {
                            if (String.Compare(encoding.First(), "gzip", true) == 0)
                            {
                                using (Stream streamGzip = new GZipStream(stream, CompressionMode.Decompress))
                                {
                                    await ParseStreamIntoChunk(streamGzip, chunk).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                await ParseStreamIntoChunk(stream, chunk).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            await ParseStreamIntoChunk(stream, chunk).ConfigureAwait(false);
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error($"Exception when trying to parse chunk {e.Message}", e);
                        if ((maxRetry <= 0) || (retryCount < maxRetry))
                        {
                            retry = true;
                            // reset the chunk before retry in case there could be garbage
                            // data left from last attempt
                            chunk.ResetForRetry();
                            await Task.Delay(TimeSpan.FromSeconds(backOffInSec), downloadContext.cancellationToken).ConfigureAwait(false);
                            ++retryCount;
                            // Set next backoff time
                            backOffInSec = backOffInSec * 2;
                            if (backOffInSec > HttpUtil.MAX_BACKOFF)
                            {
                                backOffInSec = HttpUtil.MAX_BACKOFF;
                            }
                        }
                        else
                        {
                            //parse error
                            throw new Exception("parse stream to Chunk error. " + e);
                        }
                    }
                }
            } while (retry);
            logger.Info($"Succeed downloading chunk #{chunk.ChunkIndex}");
            return chunk;
        }

        private async Task ParseStreamIntoChunk(Stream content, BaseResultChunk resultChunk)
        {
            IChunkParser parser = ChunkParserFactory.Instance.GetParser(resultChunk.ResultFormat, content);
            await parser.ParseChunk(resultChunk);
        }

        public void Dispose()
        {
            foreach (var chunk in chunkDatas)
            {
                SFChunkMemoryManager.Instance.ReleaseMemory(this.chunkBlockSize);
                chunk.Dispose();
            }
            logger.Info($"Returned memory and slots {this.prefetchSlot}, available memory {SFChunkMemoryManager.Instance.GetAmountOfMemoryAvailable()}");

            SFChunkMemoryManager.Instance.FreeMemoryChunks(this.prefetchSlot);

            logger.Info($"Free chuncks");

        }
    }

    class DownloadContextV3
    {
        public BaseResultChunk chunk { get; set; }

        public string qrmk { get; set; }

        public Dictionary<string, string> chunkHeaders { get; set; }

        public CancellationToken cancellationToken { get; set; }
    }

    public class SFChunkMemoryManager
    {

        private Func<long> GetMemoryAvailable = () => -1;

        private readonly object _memoryLock = new object();
        private long occupiedMemory;

        private IWaitingQueue _waitingForIdleSessionQueue = new WaitingQueue();

        private long initialMemoryAvailable;


        const long memoryBlockSize = (long)(1 >> 24);

        long?  availableMemory { get; set; }

        public static SFChunkMemoryManager Instance { get; } = new SFChunkMemoryManager();

        public void SetMemoryHandlerResolver(Func<long> memoryHandler)
        {
            GetMemoryAvailable = memoryHandler;
        }

        internal bool WaitForMemoryAvailable(long size)
        {
            var waitingTimeout = TimeSpan.FromSeconds(360);
            var beforeWaitingTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long nowTimeMillis = beforeWaitingTimeMillis;
            while (!TimeoutHelper.IsExpired(beforeWaitingTimeMillis, nowTimeMillis, waitingTimeout)) // we loop to handle the case if someone overtook us after being woken or session which we were promised has just expired
            {
                var timeoutLeftMillis = TimeoutHelper.FiniteTimeoutLeftMillis(beforeWaitingTimeMillis, nowTimeMillis, waitingTimeout);
                var successful = this._waitingForIdleSessionQueue.Wait((int)timeoutLeftMillis, CancellationToken.None);
                if (successful)
                {
                    lock (_memoryLock)
                    {
                        if (TryRetainMemory(size))
                        {
                            return true;
                        }
                    }
                }
                nowTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }

            return false;
        }

        internal void FreeMemoryChunks(int chunksAvailable)
        {
            int i = 0;
            while (i < Math.Max(chunksAvailable/2, 1))
            {
                this._waitingForIdleSessionQueue.OnResourceIncrease();
                i++;
            }
        }

        internal bool TryRetainMemory(long size)
        {
            lock (_memoryLock)
            {
                if (!availableMemory.HasValue)
                {
                    availableMemory = GetMemoryAvailable();
                    this.initialMemoryAvailable = availableMemory.Value;
                }

                if (availableMemory < 0)
                {
                    return true;
                }


                var requiredMemory = Math.Max(memoryBlockSize, size);

                if(((requiredMemory * 1.20) + this.occupiedMemory) > (availableMemory * 0.60))
                {
                    return false;
                }

                this.occupiedMemory += (long)(requiredMemory * 1.2);
                return true;
            }
        }

        internal long GetAmountOfMemoryAvailable()
        {
            if (!availableMemory.HasValue)
            {
                availableMemory = GetMemoryAvailable();
                this.initialMemoryAvailable = availableMemory.Value;

            }
            return availableMemory.Value - occupiedMemory;
        }

        internal long GetInitialMemoryAvailable()
        {
            return this.initialMemoryAvailable;
        }

        internal void ReleaseMemory(long size)
        {
            lock (_memoryLock)
            {
                this.occupiedMemory -= (long)(size * 1.2);
            }
        }
    }
}
