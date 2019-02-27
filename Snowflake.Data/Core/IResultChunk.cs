using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core
{
    public interface IResultChunk
    {
        string ExtractCell(int rowIndex, int columnIndex);

        int GetRowCount();

        int GetChunkIndex();
    }
}
