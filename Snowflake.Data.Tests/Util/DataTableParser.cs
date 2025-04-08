using Newtonsoft.Json.Linq;
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
            var token = JToken.Parse(jsonString);

            if (token.Type == JTokenType.Object)
            {
                return ParseKeyValue(token);
            }
            else if (token.Type == JTokenType.Array)
            {
                var array = token.ToObject<List<JToken>>();
                if (array[0].Type == JTokenType.Object)
                {
                    return ParseKeyValue(array[0]);
                }
                else
                {
                    var list = token.ToObject<List<string>>();
                    return string.Join(",", list);
                }
            }

            return jsonString;
        }

        private static string ParseKeyValue(JToken token)
        {
            var keyValue = token.ToObject<Dictionary<string, string>>();
            var key = keyValue.Keys.First();
            var value = keyValue[key];
            return $"{key}:{value}";
        }
    }
}
