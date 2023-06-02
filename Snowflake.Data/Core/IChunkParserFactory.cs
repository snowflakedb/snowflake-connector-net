using System.IO;

namespace Snowflake.Data.Core
{
    internal interface IChunkParserFactory
    {
        public IChunkParser GetParser(Stream stream);
    }
}
