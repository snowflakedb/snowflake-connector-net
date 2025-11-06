using System;
using System.Threading;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class ChunkDownloaderFactory
    {
        private static SFLogger s_logger = SFLoggerFactory.GetLogger<ChunkDownloaderFactory>();
        public static IChunkDownloader GetDownloader(QueryExecResponseData responseData,
                                                     SFBaseResultSet resultSet,
                                                     CancellationToken cancellationToken)
        {
            switch (SFConfiguration.Instance().GetChunkDownloaderVersion())
            {
                case 1:
                    s_logger.Warn("V1 version of ChunkDownloader is deprecated. Using the V3 version.");
                    return new SFBlockingChunkDownloaderV3(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet,
                        responseData.queryResultFormat);
                case 2:
                    s_logger.Warn("V2 version of ChunkDownloader is deprecated. Using the V3 version.");
                    return new SFBlockingChunkDownloaderV3(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet,
                        responseData.queryResultFormat);
                case 3:
                    return new SFBlockingChunkDownloaderV3(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet,
                        responseData.queryResultFormat);
                default:
                    throw new Exception("Unsupported Chunk Downloader version specified in the SFConfiguration");
            }
        }
    }
}
