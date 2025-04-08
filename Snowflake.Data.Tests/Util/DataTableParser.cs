using System;
using System.Data;

namespace Snowflake.Data.Tests.Util
{
    public static class DataTableParser
    {
        internal static string GetFirstRowValue(DataTable dt, string colName)
        {
            return dt.Rows[0][colName].ToString()
            .Replace("\"", String.Empty)
            .Replace(" ", String.Empty)
            .Replace("{", String.Empty)
            .Replace("}", String.Empty)
            .Replace("\n", String.Empty)
            .Replace("[", String.Empty)
            .Replace("]", String.Empty);
        }
    }
}
