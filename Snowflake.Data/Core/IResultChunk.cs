﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core
{
    public enum ResultFormat
    {
        Json,
        Arrow
    }
    
    public interface IResultChunk
    {
        UTF8Buffer ExtractCell(int rowIndex, int columnIndex);

        int GetRowCount();

        int GetChunkIndex();
    }
}
