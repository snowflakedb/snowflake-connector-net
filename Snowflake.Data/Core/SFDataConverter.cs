using System;
using System.ComponentModel;
using System.Data;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core
{
    public enum SFDataType
    {
        FIXED, REAL, TEXT, DATE, VARIANT, TIMESTAMP_LTZ, TIMESTAMP_NTZ,
        TIMESTAMP_TZ, OBJECT, BINARY, TIME, BOOLEAN
    }

    /*enum CSharpType
    {
        BOOLEAN, BYTE, CHAR, DATETIME, DECIMAL, DOUBLE, FLOAT, GUID, INT16, INT32, INT64, STRING 
    }*/

    class SFDataConverter
    {
        static private DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static internal Object convertToCSharpVal(string srcVal, SFDataType srcType, Type destType)
        {
            if (destType == typeof(Int16) 
                || destType == typeof(Int32)
                || destType == typeof(Int64)
                || destType == typeof(Guid)
                || destType == typeof(Double)
                || destType == typeof(float)
                || destType == typeof(Decimal))
            {
                var typeConverter = TypeDescriptor.GetConverter(destType);
                return typeConverter.ConvertFrom(srcVal);
            }
            else if (destType == typeof(Boolean))
            {
                return String.Compare(srcVal, "1", true) == 0 
                    || String.Compare(srcVal, "true", true) == 0;
            }
            else if (destType == typeof(string))
            {
                return srcVal;
            }
            else if (destType == typeof(DateTime))
            {
                return convertToDateTime(srcVal, srcType);
            }
            else
            {
                return "";
            }
        }

        static private DateTime convertToDateTime(string srcVal, SFDataType srcType)
        {
            if (srcType == SFDataType.DATE)
            {
                long srcValLong = Int64.Parse(srcVal);
                return unixEpoch.AddDays(srcValLong);
            }
            else if (srcType == SFDataType.TIME || srcType == SFDataType.TIMESTAMP_NTZ)
            {
                Tuple<long, long> secAndNsec = extractTimestamp(srcVal);
                return unixEpoch.AddTicks((long)(secAndNsec.Item1 * 1000 * 1000 * 1000 + secAndNsec.Item2) / 100);
            }
            else
            {
                return DateTime.Today;
            }
        }

        private static Tuple<long, long> extractTimestamp(string srcVal)
        {
            int dotIndex = srcVal.IndexOf('.');
            if (dotIndex == -1)
            {
                return Tuple.Create(Int64.Parse(srcVal), (long)0);
            }
            else
            {
                return Tuple.Create(Int64.Parse(srcVal.Substring(0, dotIndex)), 
                    Int64.Parse(srcVal.Substring(dotIndex+1, srcVal.Length-dotIndex-1)));
            }
        }

            
        static internal Tuple<string, string> csharpTypeValToSfTypeVal(DbType srcType, object srcVal)
        {
            string destType;
            string destVal;

            switch(srcType)
            {
                case DbType.Decimal:
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                case DbType.VarNumeric:
                    destType = SFDataType.FIXED.ToString();
                    destVal = srcVal.ToString();
                    break;

                case DbType.Boolean:
                    destType = SFDataType.BOOLEAN.ToString();
                    destVal = srcVal.ToString();
                    break;

                case DbType.Double:
                    destType = SFDataType.REAL.ToString();
                    destVal = srcVal.ToString();
                    break;

                case DbType.Guid:
                case DbType.String:
                case DbType.StringFixedLength:
                    destType = SFDataType.TEXT.ToString();
                    destVal = srcVal.ToString();
                    break;

                case DbType.Date:
                    destType = SFDataType.DATE.ToString();
                    if (srcVal.GetType() != typeof(DateTime))
                    {
                        throw new SFException();
                    }
                    else
                    {
                        long millis = (long)((DateTime)srcVal).ToUniversalTime().Subtract(
                            unixEpoch).TotalMilliseconds;
                        destVal = millis.ToString();
                    }
                    break;

                case DbType.Time:
                    destType = SFDataType.TIME.ToString();
                    if (srcVal.GetType() != typeof(DateTime))
                    {
                        throw new SFException();
                    }
                    else
                    {
                        DateTime srcDt = ((DateTime)srcVal);
                        long nanoSinceMidNight = (long)(srcDt.Ticks - srcDt.Date.Ticks) * 100L; 

                        destVal = nanoSinceMidNight.ToString();
                    }
                    break;

                case DbType.DateTime:
                    destType = SFDataType.TIMESTAMP_NTZ.ToString();
                    if (srcVal.GetType() != typeof(DateTime))
                    {
                        throw new SFException();
                    }
                    else
                    {
                        DateTime srcDt = ((DateTime)srcVal);
                        destVal = ((long)(srcDt.Subtract(unixEpoch).Ticks * 100)).ToString();
                    }
                    break;

                default:
                    throw new SFException();
            }
            return Tuple.Create(destType, destVal);
        }
    }
}
