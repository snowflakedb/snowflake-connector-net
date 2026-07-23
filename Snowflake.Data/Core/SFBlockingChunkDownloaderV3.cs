using System;
using System.IO.Compression;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal sealed class SFBlockingChunkDownloaderV3 : IChunkDownloader
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();

        private readonly List<BaseResultChunk> _chunkDatas = new();

        private readonly string _qrmk;

        // External cancellation token, used to stop download
        private readonly CancellationToken _externalCancellationToken;

        private readonly int _prefetchSlot;
        private readonly IRestRequester _restRequester;
        private readonly SFSessionProperties _sessionProperties;
        private readonly Dictionary<string, string> _chunkHeaders;
        private readonly SFBaseResultSet _resultSet;
        private readonly List<ExecResponseChunk> _chunkInfos;
        private readonly List<Task<BaseResultChunk>> _taskQueues;

        private int _nextChunkToDownloadIndex;
        private int _nextChunkToConsumeIndex;

        public SFBlockingChunkDownloaderV3(int colCount,
            List<ExecResponseChunk> chunkInfos, string qrmk,
            Dictionary<string, string> chunkHeaders,
            CancellationToken cancellationToken,
            SFBaseResultSet resultSet,
            ResultFormat resultFormat)
        {
            _qrmk = qrmk;
            _chunkHeaders = chunkHeaders;
            _nextChunkToDownloadIndex = 0;
            _resultSet = resultSet;
            _restRequester = resultSet.sfStatement.SfSession.restRequester;
            _sessionProperties = resultSet.sfStatement.SfSession.properties;
            _prefetchSlot = Math.Min(chunkInfos.Count, GetPrefetchThreads(resultSet));
            _chunkInfos = chunkInfos;
            _nextChunkToConsumeIndex = 0;
            _taskQueues = [];
            _externalCancellationToken = cancellationToken;

            for (var i = 0; i < _prefetchSlot; i++)
            {
                var resultChunk =
                    resultFormat == ResultFormat.ARROW ? (BaseResultChunk)
                        new ArrowResultChunk(colCount) :
                        new SFReusableChunk(colCount);

                resultChunk.Reset(chunkInfos[_nextChunkToDownloadIndex], _nextChunkToDownloadIndex);
                _chunkDatas.Add(resultChunk);

                _taskQueues.Add(DownloadChunkAsync(new DownloadContextV3()
                {
                    Chunk = resultChunk,
                    Qrmk = _qrmk,
                    ChunkHeaders = _chunkHeaders,
                    CancellationToken = _externalCancellationToken
                }));

                _nextChunkToDownloadIndex++;
            }
        }

        private static int GetPrefetchThreads(SFBaseResultSet resultSet)
        {
            var sessionParameters = resultSet.sfStatement.SfSession.ParameterMap;
            var val = (string)sessionParameters[SFSessionParameter.CLIENT_PREFETCH_THREADS];
            return int.Parse(val);
        }

        public async Task<BaseResultChunk> GetNextChunkAsync()
        {
            s_logger.Debug($"NextChunkToConsume: {_nextChunkToConsumeIndex}, NextChunkToDownload: {_nextChunkToDownloadIndex}");
            if (_nextChunkToConsumeIndex < _chunkInfos.Count)
            {
                var chunk = _taskQueues[_nextChunkToConsumeIndex % _prefetchSlot];

                if (_nextChunkToDownloadIndex < _chunkInfos.Count && _nextChunkToConsumeIndex > 0)
                {
                    var reusableChunk = _chunkDatas[_nextChunkToDownloadIndex % _prefetchSlot];
                    reusableChunk.Reset(_chunkInfos[_nextChunkToDownloadIndex], _nextChunkToDownloadIndex);

                    _taskQueues[_nextChunkToDownloadIndex % _prefetchSlot] = DownloadChunkAsync(new DownloadContextV3()
                    {
                        Chunk = reusableChunk,
                        Qrmk = _qrmk,
                        ChunkHeaders = _chunkHeaders,
                        CancellationToken = _externalCancellationToken
                    });
                    _nextChunkToDownloadIndex++;

                    // in case of one slot we need to return the chunk already downloaded
                    if (_prefetchSlot == 1)
                    {
                        chunk = _taskQueues[0];
                    }
                }
                _nextChunkToConsumeIndex++;
                return await chunk.ConfigureAwait(false);
            }
            else
            {
                return null;
            }
        }

        private async Task<BaseResultChunk> DownloadChunkAsync(DownloadContextV3 downloadContext)
        {
            var chunk = downloadContext.Chunk;
            var backOffInSec = 1;
            var retry = false;
            var retryCount = 0;
            var maxRetry = int.Parse(_sessionProperties[SFSessionProperty.MAXHTTPRETRIES]);

            do
            {
                retry = false;

                var downloadRequest =
                    new S3DownloadRequest
                    {
                        Url = new UriBuilder(chunk.Url).Uri,
                        qrmk = downloadContext.Qrmk,
                        // s3 download request timeout to one hour
                        RestTimeout = TimeSpan.FromHours(1),
                        HttpTimeout = Timeout.InfiniteTimeSpan, // Disable timeout for each request
                        chunkHeaders = downloadContext.ChunkHeaders,
                        sid = _resultSet.sfStatement.SfSession.sessionId
                    };

                using var httpResponse = await _restRequester.GetAsync(downloadRequest, downloadContext.CancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
#if NET6_0_OR_GREATER
                await using var stream = await httpResponse.Content.ReadAsStreamAsync(_externalCancellationToken).ConfigureAwait(false);
#else
                using var stream = await httpResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);
#endif

                // retry on chunk downloading since the retry logic in HttpClient.RetryHandler
                // doesn't cover this. The GET request could be succeeded but network error
                // still could happen during reading chunk data from stream and that needs
                // retry as well.
                try
                {
                    if (httpResponse.Content.Headers.TryGetValues("Content-Encoding", out var encoding))
                    {
                        if (string.Compare(encoding.First(), "gzip", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            using var streamGzip = new GZipStream(stream, CompressionMode.Decompress);
                            await ParseStreamIntoChunkAsync(streamGzip, chunk, downloadContext.CancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            await ParseStreamIntoChunkAsync(stream, chunk, downloadContext.CancellationToken).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await ParseStreamIntoChunkAsync(stream, chunk, downloadContext.CancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    if (!downloadContext.CancellationToken.IsCancellationRequested && (maxRetry <= 0 || retryCount < maxRetry))
                    {
                        s_logger.Debug($"Retry {retryCount}/{maxRetry} of parse stream to chunk error: " + e.Message);
                        retry = true;
                        // reset the chunk before retry in case there could be garbage
                        // data left from last attempt
                        chunk.ResetForRetry();
                        await Task.Delay(TimeSpan.FromSeconds(backOffInSec), downloadContext.CancellationToken).ConfigureAwait(false);
                        ++retryCount;
                        // Set next backoff time
                        backOffInSec *= 2;
                        if (backOffInSec > HttpUtil.MAX_BACKOFF)
                        {
                            backOffInSec = HttpUtil.MAX_BACKOFF;
                        }
                    }
                    else
                    {
                        //parse error
                        s_logger.Error("Failed retries of parse stream to chunk error: " + e.Message);
                        throw new Exception("Parse stream to chunk error: " + e.Message);
                    }
                }
            } while (retry);
            s_logger.Debug($"Succeed downloading chunk #{chunk.ChunkIndex}");
            return chunk;
        }

        private static async Task ParseStreamIntoChunkAsync(Stream content, BaseResultChunk resultChunk, CancellationToken cancellationToken)
        {
            var parser = ChunkParserFactory.Instance.GetParser(resultChunk.ResultFormat, content);
            await parser.ParseChunkAsync(resultChunk, cancellationToken).ConfigureAwait(false);
        }
    }

    internal class DownloadContextV3
    {
        public BaseResultChunk Chunk { get; set; }

        public string Qrmk { get; set; }

        public Dictionary<string, string> ChunkHeaders { get; set; }

        public CancellationToken CancellationToken { get; set; }
    }
}
