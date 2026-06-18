using System;
using System.Globalization;

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
                // Parse without timezone conversion - NTZ should be returned as-is
                // Use RoundtripKind to avoid automatic conversion to local time
                var dateTimeNoTz = DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                // NTZ should always be Unspecified kind (no timezone info)
                dateTimeNoTz = DateTime.SpecifyKind(dateTimeNoTz, DateTimeKind.Unspecified);

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

                var parsedDateTimeOffset = DateTimeOffset.Parse(value);

                // Use ToLocalTime() for local timezone to maintain exact backward compatibility
                // (TimeZoneInfo.ConvertTimeFromUtc behaves differently for historical dates)
                if (sessionTimezone.Equals(TimeZoneInfo.Local))
                {
                    var localDateTimeOffset = parsedDateTimeOffset.ToLocalTime();
                    if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                    {
                        return localDateTimeOffset;
                    }
                    if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                    {
                        return localDateTimeOffset.LocalDateTime;
                    }
                }
                else
                {
                    var dateTimeInSessionTz = TimeZoneInfo.ConvertTimeFromUtc(parsedDateTimeOffset.UtcDateTime, sessionTimezone);
                    if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                    {
                        return new DateTimeOffset(dateTimeInSessionTz, sessionTimezone.GetUtcOffset(dateTimeInSessionTz));
                    }
                    if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                    {
                        // Return the DateTime in session timezone (marked as Local for API compatibility)
                        return DateTime.SpecifyKind(dateTimeInSessionTz, DateTimeKind.Local);
                    }
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
