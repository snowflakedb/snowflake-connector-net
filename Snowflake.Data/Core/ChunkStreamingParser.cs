/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    using Snowflake.Data.Client;

    class ChunkStreamingParser : IChunkParser
    {
        private readonly Stream stream;

        internal ChunkStreamingParser(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(IResultChunk chunk)
        {
            // parse results row by row
            using (StreamReader sr = new StreamReader(stream))
            using (JsonTextReader jr = new JsonTextReader(sr) { DateParseHandling = DateParseHandling.None })
            {
                int row = 0;
                int col = 0;

                var outputMatrix = new string[chunk.GetRowCount(), ((SFResultChunk)chunk).colCount];

                while (jr.Read())
                {
                    switch (jr.TokenType)
                    {
                        case JsonToken.StartArray:
                        case JsonToken.None:
                            break;

                        case JsonToken.EndArray:
                            if (col > 0)
                            {
                                col = 0;
                                row++;
                            }

                            break;

                        case JsonToken.Null:
                            outputMatrix[row, col++] = null;
                            break;

                        case JsonToken.String:
                            outputMatrix[row, col++] = (string)jr.Value;
                            break;

                        default:
                            throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected token type: {jr.TokenType}");
                    }
                }

                ((SFResultChunk)chunk).rowSet = outputMatrix;
            }
        }
    }
}
