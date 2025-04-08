using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Snowflake.Data.Tests.Util
{
    public static class DataTableParser
    {
        internal static string GetFirstRowValue(DataTable dt, string colName)
        {
            var jsonString = dt.Rows[0][colName].ToString();
            if (jsonString.Contains(':'))
            {
                var keyValue = JsonConvert.DeserializeObject<Dictionary<string, string>>(jsonString.Replace("[", "").Replace("]", ""));
                var key = keyValue.Keys.First();
                var Value = keyValue[key];
                return $"{key}:{Value}";
            }
            else if (jsonString.Contains(','))
            {
                var array = JsonConvert.DeserializeObject<List<string>>(jsonString);
                return string.Join(",", array);
            }
            return jsonString;
        }
    }
}
