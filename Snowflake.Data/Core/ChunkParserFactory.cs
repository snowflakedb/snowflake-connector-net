/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using System.Collections.Specialized;
using System.Configuration;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Core
{
    class ChunkParserFactory
    {
        public static IChunkParser GetParser(Stream stream)
        {
            if (!SFConfiguration.Instance().UseV2JsonParser)
            {
                return new ChunkDeserializer(stream);
            }
            else
            {
                return new ChunkStreamingParser(stream);
            }  
        }
    }
}
