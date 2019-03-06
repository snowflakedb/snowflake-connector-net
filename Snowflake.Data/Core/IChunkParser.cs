/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;

namespace Snowflake.Data.Core
{
    interface IChunkParser
    {
        /// <summary>
        ///     Parse source data stream, result will be store into SFResultChunk.rowset
        /// </summary>
        /// <param name="chunk"></param>
        void ParseChunk(IResultChunk chunk);
    }
}
