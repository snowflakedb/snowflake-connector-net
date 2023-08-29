/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Core
{
    class ChunkDownloaderFactory
    {
        public static IChunkDownloader GetDownloader(QueryExecResponseData responseData,
                                                     SFBaseResultSet resultSet,
                                                     CancellationToken cancellationToken)
        {
            switch (SFConfiguration.Instance().GetChunkDownloaderVersion())
            {
                case 1:
                    return new SFBlockingChunkDownloader(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet);
                case 2:
                    return new SFChunkDownloaderV2(responseData.rowType.Count,
                        responseData.chunks,
                        responseData.qrmk,
                        responseData.chunkHeaders,
                        cancellationToken,
                        resultSet.sfStatement.SfSession.restRequester);
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
