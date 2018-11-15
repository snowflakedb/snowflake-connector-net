using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    class ChunkDeserializer : IChunkParser
    {
        private static JsonSerializer JsonSerializer = new JsonSerializer();

        private readonly Stream stream;

        internal ChunkDeserializer(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(SFResultChunk chunk)
        {
            // parse results row by row
            using (StreamReader sr = new StreamReader(stream))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                chunk.rowSet = JsonSerializer.Deserialize<string[,]>(jr);
            }
        }
    }
}
