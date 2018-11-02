/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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
    class SFChunkDownloader : IChunkDownloader
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFChunkDownloader>();

        private List<SFResultChunk> chunks;

        private string qrmk;
        
        private int nextChunkToDownloadIndex;
        
        // External cancellation token, used to stop donwload
        private CancellationToken externalCancellationToken;

        //TODO: parameterize prefetch slot
        const int prefetchSlot = 2;

        private static IRestRequest restRequest = RestRequestImpl.Instance;

        private static JsonSerializer jsonSerializer = new JsonSerializer();

        private Dictionary<string, string> chunkHeaders;

        public SFChunkDownloader(int colCount, List<ExecResponseChunk>chunkInfos, string qrmk, 
            Dictionary<string, string> chunkHeaders, CancellationToken cancellationToken)
        {
            this.qrmk = qrmk;
            this.chunkHeaders = chunkHeaders;
            this.chunks = new List<SFResultChunk>();
            this.nextChunkToDownloadIndex = 0;

            var idx = 0;
            foreach(ExecResponseChunk chunkInfo in chunkInfos)
            {
                this.chunks.Add(new SFResultChunk(chunkInfo.url, chunkInfo.rowCount, colCount, idx++));
            }
            logger.Info($"Total chunk number: {chunks.Count}");

            FillDownloads();
        }

        private BlockingCollection<Task<SFResultChunk>> _downloadTasks;
        
        private void FillDownloads()
        {
            _downloadTasks = new BlockingCollection<Task<SFResultChunk>>(prefetchSlot);

            Task.Run(() =>
            {
                foreach (var c in chunks)
                {
                    _downloadTasks.Add(DownloadChunkAsync(new DownloadContext()
                    {
                        chunk = c,
                        chunkIndex = nextChunkToDownloadIndex,
                        qrmk = this.qrmk,
                        chunkHeaders = this.chunkHeaders,
                        cancellationToken = this.externalCancellationToken
                    }));
                }

                _downloadTasks.CompleteAdding();
            });
        }
        
        public Task<SFResultChunk> GetNextChunkAsync()
        {
            return _downloadTasks.IsCompleted ? Task.FromResult<SFResultChunk>(null) : _downloadTasks.Take();
        }
        
        private async Task<SFResultChunk> DownloadChunkAsync(DownloadContext downloadContext)
        {
            logger.Info($"Start donwloading chunk #{downloadContext.chunkIndex}");
            SFResultChunk chunk = downloadContext.chunk;

            chunk.downloadState = DownloadState.IN_PROGRESS;

            S3DownloadRequest downloadRequest = new S3DownloadRequest()
            {
                uri = new UriBuilder(chunk.url).Uri,
                qrmk = downloadContext.qrmk,
                // s3 download request timeout to one hour
                timeout = TimeSpan.FromHours(1),
                httpRequestTimeout = TimeSpan.FromSeconds(16),
                chunkHeaders = downloadContext.chunkHeaders
            };

            var httpResponse = await restRequest.GetAsync(downloadRequest, downloadContext.cancellationToken);
            Stream stream = httpResponse.Content.ReadAsStreamAsync().Result;
            IEnumerable<string> encoding;
            //TODO this shouldn't be required.
            if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out encoding))
            {
                if (String.Compare(encoding.First(), "gzip", true) == 0)
                {
                    stream = new GZipStream(stream, CompressionMode.Decompress);
                }
            }

            parseStreamIntoChunk(stream, chunk);

            chunk.downloadState = DownloadState.SUCCESS;
            logger.Info($"Succeed downloading chunk #{downloadContext.chunkIndex}");

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
        private static void parseStreamIntoChunk(Stream content, SFResultChunk resultChunk)
        {
            Stream openBracket = new MemoryStream(Encoding.UTF8.GetBytes("["));
            Stream closeBracket = new MemoryStream(Encoding.UTF8.GetBytes("]"));

            Stream concatStream = new ConcatenatedStream(new Stream[3] { openBracket, content, closeBracket});

            // parse results row by row
            using (StreamReader sr = new StreamReader(concatStream))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                resultChunk.rowSet = jsonSerializer.Deserialize<string[,]>(jr);
            }
        }
    }

    class DownloadContext
    {
        public SFResultChunk chunk { get; set; }

        public int chunkIndex { get; set; }

        public string qrmk { get; set; }

        public Dictionary<string, string> chunkHeaders { get; set; }

        public CancellationToken cancellationToken { get; set; }
    }
}
