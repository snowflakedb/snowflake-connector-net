using System;

namespace Snowflake.Data.Core
{
    public enum ResultFormat
    {
        JSON,
        ARROW
    }

    internal interface IResultChunk
    {
        [Obsolete("ExtractCell with rowIndex is deprecated", false)]
        UTF8Buffer ExtractCell(int rowIndex, int columnIndex);

        int GetRowCount();

        int GetChunkIndex();
    }
}
