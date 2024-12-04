/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Core
{
    class ChunkParserFactory : IChunkParserFactory
    {
        private static ILogger s_logger = SFLoggerFactory.GetCustomLogger<ChunkParserFactory>();
        public static IChunkParserFactory Instance = new ChunkParserFactory();

        public IChunkParser GetParser(ResultFormat resultFormat, Stream stream)
        {
            if (resultFormat == ResultFormat.ARROW)
                return new ArrowChunkParser(stream);
            
            switch (SFConfiguration.Instance().GetChunkParserVersion())
            {
                case 1:
                    s_logger.LogWarning("V1 version of ChunkParser is deprecated. Using the V3 version.");
                    return new ReusableChunkParser(stream);
                case 2:
                    s_logger.LogWarning("V2 version of ChunkParser is deprecated. Using the V3 version.");
                    return new ReusableChunkParser(stream);
                case 3:
                    return new ReusableChunkParser(stream);
                default:
                    throw new Exception("Unsupported Chunk Parser version specified in the SFConfiguration");
            }
        }
    }
}
