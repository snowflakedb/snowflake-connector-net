/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Core
{
    class ChunkDownloaderFactory
    {
        private static ILogger s_logger = SFLoggerFactory.GetCustomLogger<ChunkDownloaderFactory>();
        public static IChunkDownloader GetDownloader(QueryExecResponseData responseData,
                                                     SFBaseResultSet resultSet,
                                                     CancellationToken cancellationToken)
        {
            switch (SFConfiguration.Instance().GetChunkDownloaderVersion())
            {
                case 1:
                    s_logger.LogWarning("V1 version of ChunkDownloader is deprecated. Using the V3 version.");
                    return new SFBlockingChunkDownloaderV3(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet,
                        responseData.queryResultFormat);
                case 2:
                    s_logger.LogWarning("V2 version of ChunkDownloader is deprecated. Using the V3 version.");
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
