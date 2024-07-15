/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core
{
    public enum ResultFormat
    {
        JSON,
        ARROW
    }

    public interface IResultChunk
    {
        [Obsolete("ExtractCell with rowIndex is deprecated", false)]
        UTF8Buffer ExtractCell(int rowIndex, int columnIndex);

        int GetRowCount();

        int GetChunkIndex();
    }
}