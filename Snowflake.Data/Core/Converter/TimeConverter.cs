using System;

namespace Snowflake.Data.Core.Converter
{
    internal class TimeConverter
    {
        public object Convert(string value, SFDataType timestampType, Type fieldType, TimeZoneInfo sessionTimezone)
        {
            if (fieldType == typeof(string))
            {
                return value;
            }
            if (timestampType == SFDataType.TIMESTAMP_NTZ)
            {
                var dateTimeNoTz = DateTime.Parse(value);
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTimeNoTz;
                }

                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return (DateTimeOffset)DateTime.SpecifyKind(dateTimeNoTz, DateTimeKind.Utc);
                }

                throw new StructuredTypesReadingException($"Cannot read TIMESTAMP_NTZ into {fieldType} type");
            }

            if (timestampType == SFDataType.TIMESTAMP_TZ)
            {
                var dateTimeOffset = DateTimeOffset.Parse(value);
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return dateTimeOffset;
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTimeOffset.UtcDateTime;
                }

                throw new StructuredTypesReadingException($"Cannot read TIMESTAMP_TZ into {fieldType} type");
            }
            if (timestampType == SFDataType.TIMESTAMP_LTZ)
            {
                if (sessionTimezone == null)
                {
                    throw new StructuredTypesReadingException("Session timezone is required for TIMESTAMP_LTZ conversion");
                }

                var utcDateTimeOffset = DateTimeOffset.Parse(value);
                var localDateTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTimeOffset.UtcDateTime, sessionTimezone);
                var dateTimeOffsetInSessionTz = new DateTimeOffset(localDateTime, sessionTimezone.GetUtcOffset(localDateTime));

                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return dateTimeOffsetInSessionTz;
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return DateTime.SpecifyKind(localDateTime, DateTimeKind.Local);
                }
                throw new StructuredTypesReadingException($"Cannot read TIMESTAMP_LTZ into {fieldType} type");
            }
            if (timestampType == SFDataType.TIME)
            {
                if (fieldType == typeof(TimeSpan) || fieldType == typeof(TimeSpan?))
                {
                    return TimeSpan.Parse(value);
                }
                throw new StructuredTypesReadingException($"Cannot read TIME into {fieldType} type");
            }
            if (timestampType == SFDataType.DATE)
            {
                var dateTime = DateTime.Parse(value);
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return dateTime;
                }
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return (DateTimeOffset)DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                }
                throw new StructuredTypesReadingException($"Cannot not read DATE into {fieldType} type");
            }
            throw new StructuredTypesReadingException($"Unsupported conversion from {timestampType.ToString()} to {fieldType} type");
        }
    }
}
