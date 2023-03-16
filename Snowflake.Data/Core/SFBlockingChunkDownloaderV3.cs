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
    class SFBlockingChunkDownloaderV3 : IChunkDownloader
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();

        private List<SFReusableChunk> chunkDatas = new List<SFReusableChunk>();

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

        private readonly List<Task<IResultChunk>> taskQueues;

        public SFBlockingChunkDownloaderV3(int colCount,
            List<ExecResponseChunk> chunkInfos, string qrmk,
            Dictionary<string, string> chunkHeaders,
            CancellationToken cancellationToken,
            SFBaseResultSet ResultSet)
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
            this.taskQueues = new List<Task<IResultChunk>>();
            externalCancellationToken = cancellationToken;

            for (int i=0; i<prefetchSlot; i++)
            {
                SFReusableChunk reusableChunk = new SFReusableChunk(colCount);
                reusableChunk.Reset(chunkInfos[nextChunkToDownloadIndex], nextChunkToDownloadIndex);
                chunkDatas.Add(reusableChunk);

                taskQueues.Add(DownloadChunkAsync(new DownloadContextV3()
                {
                    chunk = reusableChunk,
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


        /*public Task<IResultChunk> GetNextChunkAsync()
        {
            return _downloadTasks.IsCompleted ? Task.FromResult<SFResultChunk>(null) : _downloadTasks.Take();
        }*/

        public async Task<IResultChunk> GetNextChunkAsync()
        {
            logger.Info($"NextChunkToConsume: {nextChunkToConsumeIndex}, NextChunkToDownload: {nextChunkToDownloadIndex}");
            if (nextChunkToConsumeIndex < chunkInfos.Count)
            {
                Task<IResultChunk> chunk = taskQueues[nextChunkToConsumeIndex % prefetchSlot];

                if (nextChunkToDownloadIndex < chunkInfos.Count && nextChunkToConsumeIndex > 0)
                {
                    SFReusableChunk reusableChunk = chunkDatas[nextChunkToDownloadIndex % prefetchSlot];
                    reusableChunk.Reset(chunkInfos[nextChunkToDownloadIndex], nextChunkToDownloadIndex);

                    taskQueues[nextChunkToDownloadIndex % prefetchSlot] = DownloadChunkAsync(new DownloadContextV3()
                    {
                        chunk = reusableChunk,
                        qrmk = this.qrmk,
                        chunkHeaders = this.chunkHeaders,
                        cancellationToken = externalCancellationToken
                    });
                    nextChunkToDownloadIndex++;
                }

                nextChunkToConsumeIndex++;
                return await chunk;
            }
            else
            {
                return await Task.FromResult<IResultChunk>(null);
            }
        }

        private async Task<IResultChunk> DownloadChunkAsync(DownloadContextV3 downloadContext)
        {
            //logger.Info($"Start downloading chunk #{downloadContext.chunkIndex}");
            SFReusableChunk chunk;
            bool retry = false;
            int retryCount = 0;

            //this is used for test case
            bool forceParseError = Boolean.Parse((string)sessionProperies[SFSessionProperty.FORCEPARSEERROR]);

            do
            {
                int backOffInSec = 1;
                retry = false;
                chunk = downloadContext.chunk;

                S3DownloadRequest downloadRequest =
                    new S3DownloadRequest()
                    {
                        Url = new UriBuilder(chunk.Url).Uri,
                        qrmk = downloadContext.qrmk,
                        // s3 download request timeout to one hour
                        RestTimeout = TimeSpan.FromHours(1),
                        HttpTimeout = Timeout.InfiniteTimeSpan, // Disable timeout for each request
                        chunkHeaders = downloadContext.chunkHeaders
                    };

                using (var httpResponse = await _RestRequester.GetAsync(downloadRequest, downloadContext.cancellationToken)
                               .ConfigureAwait(continueOnCapturedContext: false))
                using (Stream stream = await httpResponse.Content.ReadAsStreamAsync()
                    .ConfigureAwait(continueOnCapturedContext: false))
                {
                    //TODO this shouldn't be required.
                    try
                    {
                        if(forceParseError)
                        {
                            throw new Exception("json parsing error.");
                        }
                        IEnumerable<string> encoding;
                        if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out encoding))
                        {
                            if (String.Compare(encoding.First(), "gzip", true) == 0)
                            {
                                Stream stream_gzip = new GZipStream(stream, CompressionMode.Decompress);
                                await ParseStreamIntoChunk(stream_gzip, chunk);
                            }
                            else
                            {
                                await ParseStreamIntoChunk(stream, chunk);
                            }
                        }
                        else
                        {
                            await ParseStreamIntoChunk(stream, chunk);
                        }
                    }
                    catch (Exception e)
                    {
                        forceParseError = false;
                        if (retryCount < HttpUtil.MAX_RETRY)
                        {
                            retry = true;
                            await Task.Delay(TimeSpan.FromSeconds(backOffInSec), downloadContext.cancellationToken).ConfigureAwait(false);
                            ++retryCount;
                            backOffInSec = backOffInSec * 2;
                        }
                        else
                        {
                            //parse error
                            throw new Exception("parse stream to Chunk error. " + e);
                        }
                    }
                }
            } while (retry);
            logger.Info($"Succeed downloading chunk #{chunk.chunkIndexToDownload}");
            return chunk;
        }

        /// <summary>
        ///     Content from s3 in format of 
        ///     ["val1", "val2", null, ...],
        ///     ["val3", "val4", null, ...],
        ///     ...
        ///     To parse it as a json, we need to preappend '[' and append ']' to the stream 
        /// </summary>
        /// <param name="content"></param>
        /// <param name="resultChunk"></param>
        private async Task ParseStreamIntoChunk(Stream content, IResultChunk resultChunk)
        {
            IChunkParser parser = new ReusableChunkParser(content);
            await parser.ParseChunk(resultChunk);
        }
    }

    class DownloadContextV3
    {
        public SFReusableChunk chunk { get; set; }

        public string qrmk { get; set; }

        public Dictionary<string, string> chunkHeaders { get; set; }

        public CancellationToken cancellationToken { get; set; }
    }
}
