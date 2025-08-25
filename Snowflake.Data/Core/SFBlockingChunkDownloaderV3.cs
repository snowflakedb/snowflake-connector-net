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
    class SFBlockingChunkDownloaderV3 : IChunkDownloader
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();

        private List<BaseResultChunk> chunkDatas = new List<BaseResultChunk>();

        private string qrmk;

        private int nextChunkToDownloadIndex;

        private int nextChunkToConsumeIndex;

        // External cancellation token, used to stop donwload
        private CancellationToken externalCancellationToken;

        private readonly int prefetchSlot;

        private readonly IRestRequester _RestRequester;

        private readonly SFSessionProperties sessionProperies;

        private Dictionary<string, string> chunkHeaders;

        private readonly SFBaseResultSet ResultSet;

        private readonly List<ExecResponseChunk> chunkInfos;

        private readonly List<Task<BaseResultChunk>> taskQueues;

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
            this.nextChunkToConsumeIndex = 0;
            this.taskQueues = new List<Task<BaseResultChunk>>();
            externalCancellationToken = cancellationToken;

            for (int i = 0; i < prefetchSlot; i++)
            {
                BaseResultChunk resultChunk =
                    resultFormat == ResultFormat.ARROW ? (BaseResultChunk)
                        new ArrowResultChunk(colCount) :
                        new SFReusableChunk(colCount);

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

        private int GetPrefetchThreads(SFBaseResultSet resultSet)
        {
            Dictionary<SFSessionParameter, object> sessionParameters = resultSet.sfStatement.SfSession.ParameterMap;
            String val = (String)sessionParameters[SFSessionParameter.CLIENT_PREFETCH_THREADS];
            return Int32.Parse(val);
        }

        public async Task<BaseResultChunk> GetNextChunkAsync()
        {
            logger.Debug($"NextChunkToConsume: {nextChunkToConsumeIndex}, NextChunkToDownload: {nextChunkToDownloadIndex}");
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
                        if ((maxRetry <= 0) || (retryCount < maxRetry))
                        {
                            logger.Debug($"Retry {retryCount}/{maxRetry} of parse stream to chunk error: " + e.Message);
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
                            logger.Error("Failed retries of parse stream to chunk error: " + e.Message);
                            throw new Exception("Parse stream to chunk error: " + e.Message);
                        }
                    }
                }
            } while (retry);
            logger.Debug($"Succeed downloading chunk #{chunk.ChunkIndex}");
            return chunk;
        }

        private async Task ParseStreamIntoChunk(Stream content, BaseResultChunk resultChunk)
        {
            IChunkParser parser = ChunkParserFactory.Instance.GetParser(resultChunk.ResultFormat, content);
            await parser.ParseChunk(resultChunk);
        }
    }

    class DownloadContextV3
    {
        public BaseResultChunk chunk { get; set; }

        public string qrmk { get; set; }

        public Dictionary<string, string> chunkHeaders { get; set; }

        public CancellationToken cancellationToken { get; set; }
    }
}
