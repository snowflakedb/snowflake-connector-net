#nullable enable
using System.Linq;
using System.Text;

namespace Snowflake.Data.Tests.Util
{
    internal static class TestData
    {
        internal static string ByteArrayToHexString(byte[] ba)
        {
            StringBuilder hex = new StringBuilder(ba.Length * 2);
            foreach (byte b in ba)
                hex.AppendFormat("{0:x2}", b);
            return hex.ToString();
        }

        internal static T?[] NullEachNthValue<T>(T?[] sourceColumn, int nullEachNthItem) where T : struct
        {
            var destination = new T?[sourceColumn.Length];
            foreach (var rowIndex in Enumerable.Range(0, sourceColumn.Length))
                destination[rowIndex] = rowIndex % nullEachNthItem == 0 ? null : sourceColumn[rowIndex];
            return destination;
        }

        internal static T?[] NullEachNthValue<T>(T?[] sourceColumn, int nullEachNthItem) where T : class
        {
            var destination = new T?[sourceColumn.Length];
            foreach (var rowIndex in Enumerable.Range(0, sourceColumn.Length))
                destination[rowIndex] = rowIndex % nullEachNthItem == 0 ? null : sourceColumn[rowIndex];
            return destination;
        }

        internal static object?[] NullEachNthValueBesidesFirst(object?[] sourceRow, int nullEachNthItem)
        {
            object?[] ret = new object[sourceRow.Length];
            foreach (var column in Enumerable.Range(0, sourceRow.Length))
                ret[column] = column > 0 && nullEachNthItem % (column + 1) == 0 ? null : sourceRow[column];
            return ret;
        }

        internal static string RemoveBlanks(string text)
            => text.Replace("\n", "").Replace(" ", "");

    }
}
