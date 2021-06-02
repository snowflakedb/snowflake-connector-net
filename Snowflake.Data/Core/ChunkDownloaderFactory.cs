/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

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
            int ChunkDownloaderVersion = SFConfiguration.Instance().ChunkDownloaderVersion;
            if (SFConfiguration.Instance().UseV2ChunkDownloader)
                ChunkDownloaderVersion = 2;

            switch (ChunkDownloaderVersion)
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
                        resultSet.sfStatement.SfSession.InsecureMode);
                default:
                    return new SFBlockingChunkDownloaderV3(responseData.rowType.Count,
                    responseData.chunks,
                    responseData.qrmk,
                    responseData.chunkHeaders,
                    cancellationToken,
                    resultSet);
            }
        }
    }
}
