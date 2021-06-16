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
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    /// <summary>
    ///     Downloader implementation that will be blocked if main thread consume falls behind
    /// </summary>
    class SFBlockingChunkDownloader : IChunkDownloader
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloader>();

        private List<SFResultChunk> chunks;

        private string qrmk;
        
        private int nextChunkToDownloadIndex;
        
        // External cancellation token, used to stop donwload
        private CancellationToken externalCancellationToken;

        private readonly int prefetchThreads;

        private readonly IRestRequester _RestRequester;

        private Dictionary<string, string> chunkHeaders;

        private readonly SFBaseResultSet ResultSet;

        public SFBlockingChunkDownloader(int colCount, 
            List<ExecResponseChunk>chunkInfos, string qrmk, 
            Dictionary<string, string> chunkHeaders, 
            CancellationToken cancellationToken,
            SFBaseResultSet ResultSet)
        {
            this.qrmk = qrmk;
            this.chunkHeaders = chunkHeaders;
            this.chunks = new List<SFResultChunk>();
            this.nextChunkToDownloadIndex = 0;
            this.ResultSet = ResultSet;
            this._RestRequester = ResultSet.sfStatement.SfSession.restRequester;
            this.prefetchThreads = GetPrefetchThreads(ResultSet);
            externalCancellationToken = cancellationToken;

            var idx = 0;
            foreach(ExecResponseChunk chunkInfo in chunkInfos)
            {
                this.chunks.Add(new SFResultChunk(chunkInfo.url, chunkInfo.rowCount, colCount, idx++));
            }
            logger.Info($"Total chunk number: {chunks.Count}");

            FillDownloads();
        }

        private int GetPrefetchThreads(SFBaseResultSet resultSet)
        {
            Dictionary<SFSessionParameter, Object> sessionParameters = resultSet.sfStatement.SfSession.ParameterMap;
            String val = (String)sessionParameters[SFSessionParameter.CLIENT_PREFETCH_THREADS];
            return Int32.Parse(val);
        }

        private BlockingCollection<Task<IResultChunk>> _downloadTasks;
        
        private void FillDownloads()
        {
            _downloadTasks = new BlockingCollection<Task<IResultChunk>>(prefetchThreads);

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
        
        public Task<IResultChunk> GetNextChunkAsync()
        {
            if (_downloadTasks.IsCompleted)
            {
                return Task.FromResult<IResultChunk>(null);
            }
            else
            {
                return _downloadTasks.Take();
            }
        }
        
        private async Task<IResultChunk> DownloadChunkAsync(DownloadContext downloadContext)
        {
            logger.Info($"Start donwloading chunk #{downloadContext.chunkIndex}");
            SFResultChunk chunk = downloadContext.chunk;

            chunk.downloadState = DownloadState.IN_PROGRESS;

            S3DownloadRequest downloadRequest = 
                new S3DownloadRequest()
                {
                    Url = new UriBuilder(chunk.url).Uri,
                    qrmk = downloadContext.qrmk,
                    // s3 download request timeout to one hour
                    RestTimeout = TimeSpan.FromHours(1),
                    HttpTimeout = TimeSpan.FromSeconds(32),
                    chunkHeaders = downloadContext.chunkHeaders
                };


            var httpResponse = await _RestRequester.GetAsync(downloadRequest, downloadContext.cancellationToken).ConfigureAwait(false);
            Stream stream = Task.Run(async() => await (httpResponse.Content.ReadAsStreamAsync()).ConfigureAwait(false)).Result;
            IEnumerable<string> encoding;
            //TODO this shouldn't be required.
            if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out encoding))
            {
                if (String.Compare(encoding.First(), "gzip", true) == 0)
                {
                    stream = new GZipStream(stream, CompressionMode.Decompress);
                }
            }

            ParseStreamIntoChunk(stream, chunk);

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
        private void ParseStreamIntoChunk(Stream content, SFResultChunk resultChunk)
        {
            Stream openBracket = new MemoryStream(Encoding.UTF8.GetBytes("["));
            Stream closeBracket = new MemoryStream(Encoding.UTF8.GetBytes("]"));

            Stream concatStream = new ConcatenatedStream(new Stream[3] { openBracket, content, closeBracket});

            IChunkParser parser = ChunkParserFactory.GetParser(concatStream);
            parser.ParseChunk(resultChunk);
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
