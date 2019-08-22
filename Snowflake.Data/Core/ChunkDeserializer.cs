/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    class ChunkDeserializer : IChunkParser
    {
        private static JsonSerializer JsonSerializer = new JsonSerializer() { DateParseHandling = DateParseHandling.None };

        private readonly Stream stream;

        internal ChunkDeserializer(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(IResultChunk chunk)
        {
            // parse results row by row
            using (StreamReader sr = new StreamReader(stream))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                ((SFResultChunk)chunk).rowSet = JsonSerializer.Deserialize<string[,]>(jr);
            }
        }
    }
}
