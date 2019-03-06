/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    interface IChunkDownloader
    {
        Task<IResultChunk> GetNextChunkAsync();
    }
}
