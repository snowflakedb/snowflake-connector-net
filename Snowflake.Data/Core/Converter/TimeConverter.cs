using System;

namespace Snowflake.Data.Core.Converter
{
    internal class TimeConverter
    {
        public object Convert(string value, SFDataType timestampType, Type fieldType)
        {
            if (fieldType == typeof(string))
            {
                return value;
            }
            if (timestampType == SFDataType.TIMESTAMP_NTZ)
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
                    return dateTimeOffset.ToUniversalTime().DateTime.ToUniversalTime();
                }

                throw new StructuredTypesReadingException($"Cannot read TIMESTAMP_TZ into {fieldType} type");
            }
            if (timestampType == SFDataType.TIMESTAMP_LTZ)
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
                if (fieldType == typeof(DateTimeOffset) || fieldType == typeof(DateTimeOffset?))
                {
                    return DateTimeOffset.Parse(value).ToUniversalTime();
                }
                if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                {
                    return DateTime.Parse(value).ToUniversalTime();
                }
                throw new StructuredTypesReadingException($"Cannot not read DATE into {fieldType} type");
            }
            throw new StructuredTypesReadingException($"Unsupported conversion from {timestampType.ToString()} to {fieldType} type");
        }
    }
}
