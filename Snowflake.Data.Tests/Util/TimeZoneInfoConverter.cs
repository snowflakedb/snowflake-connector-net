using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Tests.Util
{
    public static class TimeZoneInfoConverter
    {
        private static Dictionary<string, string> s_mapping = TimeZoneMapping().ToDictionary(x => x.Key, x => x.Value);

        public static TimeZoneInfo FindSystemTimeZoneById(string timeZoneString)
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(timeZoneString);
            }
            catch (TimeZoneNotFoundException)
            {
                if (s_mapping.TryGetValue(timeZoneString, out var alternativeTimeZoneString))
                {
                    return TimeZoneInfo.FindSystemTimeZoneById(alternativeTimeZoneString);
                }
                throw new Exception($"Could not recognize time zone: {timeZoneString}");
            }
        }

        private static IEnumerable<KeyValuePair<string, string>> TimeZoneMapping()
        {
            yield return new KeyValuePair<string, string>("America/Los_Angeles", "Pacific Standard Time");
            yield return new KeyValuePair<string, string>("Europe/Warsaw", "Central European Standard Time");
            yield return new KeyValuePair<string, string>("Asia/Tokyo", "Tokyo Standard Time");
        }
    }


}
