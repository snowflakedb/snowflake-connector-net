using System.IO;

namespace Snowflake.Data.Core
{
    internal interface IChunkParserFactory
    {
        IChunkParser GetParser(ResultFormat resultFormat, Stream stream);
    }
}
