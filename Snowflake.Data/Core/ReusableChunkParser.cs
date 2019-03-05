/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    using Snowflake.Data.Client;

    class ReusableChunkParser : IChunkParser
    {
        private readonly Stream stream;

        internal ReusableChunkParser(Stream stream)
        {
            this.stream = stream;
        }

        public void ParseChunk(IResultChunk chunk)
        {
            SFReusableChunk rc = (SFReusableChunk)chunk;
            // parse results row by row
            using (StreamReader sr = new StreamReader(stream))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                while (jr.Read())
                {
                    switch (jr.TokenType)
                    {
                        case JsonToken.StartArray:
                        case JsonToken.None:
                        case JsonToken.EndArray:
                            break;

                        case JsonToken.Null:
                            rc.AddCell(null);
                            break;

                        case JsonToken.String:
                            rc.AddCell((string)jr.Value);
                            break;

                        default:
                            throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected token type: {jr.TokenType}");
                    }
                }
            }
        }
    }
}
