using System;
using System.IO;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class ChunkParserFactory : IChunkParserFactory
    {
        private static SFLogger s_logger = SFLoggerFactory.GetLogger<ChunkParserFactory>();
        public static IChunkParserFactory Instance = new ChunkParserFactory();

        public IChunkParser GetParser(ResultFormat resultFormat, Stream stream)
        {
            if (resultFormat == ResultFormat.ARROW)
                return new ArrowChunkParser(stream);

            switch (SFConfiguration.Instance().GetChunkParserVersion())
            {
                case 1:
                    s_logger.Warn("V1 version of ChunkParser is deprecated. Using the V3 version.");
                    return new ReusableChunkParser(stream);
                case 2:
                    s_logger.Warn("V2 version of ChunkParser is deprecated. Using the V3 version.");
                    return new ReusableChunkParser(stream);
                case 3:
                    return new ReusableChunkParser(stream);
                default:
                    throw new Exception("Unsupported Chunk Parser version specified in the SFConfiguration");
            }
        }
    }
}
