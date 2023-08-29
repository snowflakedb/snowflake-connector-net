/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Core
{
    class ChunkParserFactory : IChunkParserFactory
    {
        public static IChunkParserFactory Instance = new ChunkParserFactory();

        public IChunkParser GetParser(ResultFormat resultFormat, Stream stream)
        {
            if (resultFormat == ResultFormat.ARROW)
                return new ArrowChunkParser(stream);
            
            switch (SFConfiguration.Instance().GetChunkParserVersion())
            {
                case 1:
                    return new ChunkStreamingParser(stream);
                case 2:
                    return new ChunkDeserializer(stream);
                case 3:
                    return new ReusableChunkParser(stream);
                default:
                    throw new Exception("Unsupported Chunk Parser version specified in the SFConfiguration");
            }
        }
    }
}
