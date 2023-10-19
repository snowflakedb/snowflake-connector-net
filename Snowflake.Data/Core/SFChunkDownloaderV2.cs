/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO.Compression;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    [Obsolete("SFChunkDownloaderV2 is deprecated", false)]
    class SFChunkDownloaderV2 : IChunkDownloader
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFChunkDownloaderV2>();

        private List<SFResultChunk> chunks;

        private string qrmk;
        
        // External cancellation token, used to stop donwload
        private CancellationToken externalCancellationToken;

        //TODO: parameterize prefetch slot
        private const int prefetchSlot = 5;
        
        private readonly IRestRequester _RestRequester;
        
        private Dictionary<string, string> chunkHeaders;

        public SFChunkDownloaderV2(int colCount, List<ExecResponseChunk>chunkInfos, string qrmk, 
			Dictionary<string, string> chunkHeaders, CancellationToken cancellationToken,
            IRestRequester restRequester)
        {
            this.qrmk = qrmk;
            this.chunkHeaders = chunkHeaders;
            this.chunks = new List<SFResultChunk>();
            _RestRequester = restRequester;
            externalCancellationToken = cancellationToken;

            var idx = 0;
            foreach(ExecResponseChunk chunkInfo in chunkInfos)
            {
                this.chunks.Add(new SFResultChunk(chunkInfo.url, chunkInfo.rowCount, colCount, idx++));
            }
            logger.Info($"Total chunk number: {chunks.Count}");

            FillDownloads();
        }

        private BlockingCollection<Lazy<Task<BaseResultChunk>>> _downloadTasks;
        private ConcurrentQueue<Lazy<Task<BaseResultChunk>>> _downloadQueue;

        private void RunDownloads()
        {
            try
            {
                while (_downloadQueue.TryDequeue(out var task) && !externalCancellationToken.IsCancellationRequested)
                {
                    if (!task.IsValueCreated)
                    {
                        task.Value.Wait(externalCancellationToken);
                    }
                }
            }
            catch (Exception)
            {
                //Don't blow from background threads.
            }
        }


        private void FillDownloads()
        {
            _downloadTasks = new BlockingCollection<Lazy<Task<BaseResultChunk>>>();

            foreach (var c in chunks)
            {
                var t = new Lazy<Task<BaseResultChunk>>(() => DownloadChunkAsync(new DownloadContextV2()
                {
                    chunk = c,
                    chunkIndex = c.ChunkIndex,
                    qrmk = this.qrmk,
                    chunkHeaders = this.chunkHeaders,
                    cancellationToken = this.externalCancellationToken,
                }));

                _downloadTasks.Add(t);
            }

            _downloadTasks.CompleteAdding();

            _downloadQueue = new ConcurrentQueue<Lazy<Task<BaseResultChunk>>>(_downloadTasks);

            for (var i = 0; i < prefetchSlot && i < chunks.Count; i++)
                Task.Run(new Action(RunDownloads));

        }

        public Task<BaseResultChunk> GetNextChunkAsync()
        {
            if (_downloadTasks.IsAddingCompleted)
            {
                return Task.FromResult<BaseResultChunk>(null);
            }
            else
            {
                return _downloadTasks.Take().Value;
            }
        }
        
        private async Task<BaseResultChunk> DownloadChunkAsync(DownloadContextV2 downloadContext)
        {
            logger.Info($"Start downloading chunk #{downloadContext.chunkIndex+1}");
            BaseResultChunk chunk = downloadContext.chunk;

            S3DownloadRequest downloadRequest = new S3DownloadRequest()
            {
                Url = new UriBuilder(chunk.Url).Uri,
                qrmk = downloadContext.qrmk,
                // s3 download request timeout to one hour
                RestTimeout = TimeSpan.FromHours(1),
                HttpTimeout = TimeSpan.FromSeconds(16),
                chunkHeaders = downloadContext.chunkHeaders
            };

            Stream stream = null;
            using (var httpResponse = await _RestRequester.GetAsync(downloadRequest, downloadContext.cancellationToken).ConfigureAwait(false))
            using (stream = await httpResponse.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {

                if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out var encoding))
                {
                    if (string.Equals(encoding.First(), "gzip", StringComparison.OrdinalIgnoreCase))
                    {
                        stream = new GZipStream(stream, CompressionMode.Decompress);
                    }
                }

                ParseStreamIntoChunk(stream, chunk);
            }
            
            logger.Info($"Succeed downloading chunk #{downloadContext.chunkIndex+1}");

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
        private static void ParseStreamIntoChunk(Stream content, BaseResultChunk resultChunk)
        {
            Stream openBracket = new MemoryStream(Encoding.UTF8.GetBytes("["));
            Stream closeBracket = new MemoryStream(Encoding.UTF8.GetBytes("]"));

            Stream concatStream = new ConcatenatedStream(new Stream[3] { openBracket, content, closeBracket});

            IChunkParser parser = ChunkParserFactory.Instance.GetParser(resultChunk.ResultFormat, concatStream);
            parser.ParseChunk(resultChunk);
        }
    }

    class DownloadContextV2
    {
        public BaseResultChunk chunk { get; set; }

        public int chunkIndex { get; set; }

        public string qrmk { get; set; }

        public Dictionary<string, string> chunkHeaders { get; set; }

        public CancellationToken cancellationToken { get; set; }
    }
}
