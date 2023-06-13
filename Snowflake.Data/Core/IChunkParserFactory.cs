using System.IO;

namespace Snowflake.Data.Core
{
    internal interface IChunkParserFactory
    {
        IChunkParser GetParser(Stream stream);
    }
}
