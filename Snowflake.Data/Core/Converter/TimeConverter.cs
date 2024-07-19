using System;

namespace Snowflake.Data.Core.Converter
{
    internal enum SFTimestampType
    {
        TIME, DATE, TIMESTAMP_TZ, TIMESTAMP_LTZ, TIMESTAMP_NTZ
    }

    internal class TimeConverter
    {
        public object Convert(string value, SFTimestampType timestampType, Type fieldType)
        {
            if (fieldType == typeof(string))
            {
                return value;
            }
            if (timestampType == SFTimestampType.TIMESTAMP_NTZ)
            {
                var dateTimeUtc = DateTime.Parse(value).ToUniversalTime();
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTimeUtc;
                }

                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return (DateTimeOffset) dateTimeUtc;
                }

                throw new Exception($"Cannot not read TIMESTAMP_NTZ into {fieldType} type");
            }

            if (timestampType == SFTimestampType.TIMESTAMP_TZ)
            {
                var dateTimeOffset = DateTimeOffset.Parse(value);
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return dateTimeOffset;
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTimeOffset.ToUniversalTime().DateTime.ToUniversalTime();
                }

                throw new Exception($"Cannot not read TIMESTAMP_TZ into {fieldType} type");
            }
            if (timestampType == SFTimestampType.TIMESTAMP_LTZ)
            {
                var dateTimeOffset = DateTimeOffset.Parse(value);
                //     .ToUniversalTime();
                // var dbOffset = TimeZoneInfo.FindSystemTimeZoneById(dbTimeZone);
                // var convertedTime = TimeZoneInfo.ConvertTime(dateTimeOffset, dbOffset);
                // var x = TimeZoneInfo.ConvertTime(dateTimeOffset.UtcDateTime, dbOffset, dbOffset);
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return dateTimeOffset;
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTimeOffset.UtcDateTime;
                }
                throw new Exception($"Cannot not read TIMESTAMP_LTZ into {fieldType} type");
            }
            if (timestampType == SFTimestampType.TIME)
            {
                if (fieldType == typeof(TimeSpan) || fieldType == typeof(TimeSpan?))
                {
                    return TimeSpan.Parse(value);
                }
                throw new Exception($"Cannot not read TIME into {fieldType} type");
            }
            if (timestampType == SFTimestampType.DATE)
            {
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return DateTimeOffset.Parse(value).ToUniversalTime();
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return DateTime.Parse(value).ToUniversalTime();
                }
                throw new Exception($"Cannot not read DATE into {fieldType} type");
            }
            throw new Exception("Case not implemented yet");
        }
    }
}
