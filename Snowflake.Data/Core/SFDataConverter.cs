/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    public enum SFDataType
    {
        None, FIXED, REAL, TEXT, DATE, VARIANT, TIMESTAMP_LTZ, TIMESTAMP_NTZ,
        TIMESTAMP_TZ, OBJECT, BINARY, TIME, BOOLEAN, ARRAY
    }

    static class SFDataConverter
    {
        internal static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        internal static readonly Dictionary<Type, DbType> TypeToDbTypeMap = new Dictionary<Type, DbType>()
        {
            [typeof(byte)] = DbType.Byte,
            [typeof(sbyte)] = DbType.SByte,
            [typeof(short)] = DbType.Int16,
            [typeof(ushort)] = DbType.UInt16,
            [typeof(int)] = DbType.Int32,
            [typeof(uint)] = DbType.UInt32,
            [typeof(long)] = DbType.Int64,
            [typeof(ulong)] = DbType.UInt64,
            [typeof(float)] = DbType.Single,
            [typeof(double)] = DbType.Double,
            [typeof(decimal)] = DbType.Decimal,
            [typeof(bool)] = DbType.Boolean,
            [typeof(string)] = DbType.String,
            [typeof(char)] = DbType.StringFixedLength,
            [typeof(Guid)] = DbType.Guid,
            [typeof(DateTime)] = DbType.DateTime,
            [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
            [typeof(TimeSpan)] = DbType.Time,
            [typeof(byte[])] = DbType.Binary,
            [typeof(object)] = DbType.Object
        };

        internal static object ConvertToCSharpVal(UTF8Buffer srcVal, SFDataType srcType, Type destType)
        {
            if (srcVal == null)
                return DBNull.Value;

            try
            {
                // The most common conversions are checked first for maximum performance
                if (destType == typeof(Int64)) 
                {
                    return FastParser.FastParseInt64(srcVal.Buffer, srcVal.offset, srcVal.length);
                }
                else if (destType == typeof(Int32))
                {
                    return FastParser.FastParseInt32(srcVal.Buffer, srcVal.offset, srcVal.length);
                }
                else if (destType == typeof(decimal))
                {
                    return FastParser.FastParseDecimal(srcVal.Buffer, srcVal.offset, srcVal.length);
                }
                else if (destType == typeof(string))
                {
                    return srcVal.ToString();
                }
                else if (destType == typeof(DateTime))
                {
                    return ConvertToDateTime(srcVal, srcType);
                }
                else if (destType == typeof(TimeSpan))
                {
                    return ConvertToTimeSpan(srcVal, srcType);
                }
                else if (destType == typeof(DateTimeOffset))
                {
                    return ConvertToDateTimeOffset(srcVal, srcType);
                }
                else if (destType == typeof(Boolean))
                {
                    var val = srcVal.Buffer[srcVal.offset];
                    return val == '1' || val == 't' || val == 'T';
                }
                else if (destType == typeof(byte[]))
                {
                    return srcType == SFDataType.BINARY ?
                        HexToBytes(srcVal.ToString()) : srcVal.GetBytes();
                }
                else if (destType == typeof(Int16))
                {
                    // Use checked keyword to make sure we generate an OverflowException if conversion fails
                    int result = FastParser.FastParseInt32(srcVal.Buffer, srcVal.offset, srcVal.length);
                    return checked((Int16)result);
                }
                else if (destType == typeof(byte))
                {
                    // Use checked keyword to make sure we generate an OverflowException if conversion fails
                    int result = FastParser.FastParseInt32(srcVal.Buffer, srcVal.offset, srcVal.length);
                    return checked((byte)result);
                }
                else if (destType == typeof(double))
                {
                    return Convert.ToDouble(srcVal.ToString(), CultureInfo.InvariantCulture);
                }
                else if (destType == typeof(float))
                {
                    return Convert.ToSingle(srcVal.ToString(), CultureInfo.InvariantCulture);
                }
                else if (destType == typeof(Guid))
                {
                    return new Guid(srcVal.ToString());
                }
                else if (destType == typeof(char[]))
                {
                    byte[] data = srcType == SFDataType.BINARY ? 
                        HexToBytes(srcVal.ToString()) : srcVal.GetBytes();
                    return Encoding.UTF8.GetString(data).ToCharArray();
                }
                else
                {
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Invalid destination type.");
                }
            }
            catch (OverflowException e)
            {
                throw new OverflowException($"Error converting '{srcVal} to {destType.Name}'. Use GetString() to handle very large values", e);
            }
        }

        private static object ConvertToTimeSpan(UTF8Buffer srcVal, SFDataType srcType)
        {
            switch (srcType)
            {
                case SFDataType.TIME:
                    // Convert fractional seconds since midnight to TimeSpan
                    //  A single tick represents one hundred nanoseconds or one ten-millionth of a second. 
                    // There are 10,000 ticks in a millisecond
                    return TimeSpan.FromTicks(GetTicksFromSecondAndNanosecond(srcVal));
                default:
                    throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, srcType, typeof(TimeSpan));
            }
        }

        private static DateTime ConvertToDateTime(UTF8Buffer srcVal, SFDataType srcType)
        {
            switch (srcType)
            {
                case SFDataType.DATE:
                    long srcValLong = FastParser.FastParseInt64(srcVal.Buffer, srcVal.offset, srcVal.length);
                    return UnixEpoch.AddDays(srcValLong);

                case SFDataType.TIME:
                case SFDataType.TIMESTAMP_NTZ:
                    var tickDiff = GetTicksFromSecondAndNanosecond(srcVal);
                    return UnixEpoch.AddTicks(tickDiff);

                default:
                    throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, srcType, typeof(DateTime));
            }
        }

        private static DateTimeOffset ConvertToDateTimeOffset(UTF8Buffer srcVal, SFDataType srcType)
        {
            switch (srcType)
            {
                case SFDataType.TIMESTAMP_TZ:
                    int spaceIndex = Array.IndexOf<byte>(srcVal.Buffer, (byte)' ', srcVal.offset, srcVal.length); ;
                    if (spaceIndex == -1)
                    {
                        throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                            $"Invalid timestamp_tz value: {srcVal}");
                    }
                    else
                    {
                        spaceIndex -= srcVal.offset;
                        UTF8Buffer timeVal = new UTF8Buffer(srcVal.Buffer, srcVal.offset, spaceIndex);
                        int offset = FastParser.FastParseInt32(srcVal.Buffer, srcVal.offset + spaceIndex + 1, srcVal.length - spaceIndex - 1);
                        TimeSpan offSetTimespan = new TimeSpan((offset - 1440) / 60, 0, 0);
                        return new DateTimeOffset(UnixEpoch.Ticks + GetTicksFromSecondAndNanosecond(timeVal), TimeSpan.Zero).ToOffset(offSetTimespan);
                    }
                case SFDataType.TIMESTAMP_LTZ:                     
                    return new DateTimeOffset(UnixEpoch.Ticks +
                        GetTicksFromSecondAndNanosecond(srcVal), TimeSpan.Zero).ToLocalTime(); 

                default:
                    throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, 
                        srcType, typeof(DateTimeOffset).ToString());
            }
        }

        static int[] powersOf10 =  { 
            1, 
            10, 
            100, 
            1000, 
            10000, 
            100000, 
            1000000, 
            10000000, 
            100000000 
        };

        /// <summary>
        /// Convert the time value with the format seconds.nanoseconds to a number of
        /// Ticks. A single tick represents one hundred nanoseconds.
        /// For example, "23:59:59.123456789" is represented by 86399.123456789 and is 
        /// 863991234567 ticks (precision is maximum 7).
        /// </summary>
        /// <param name="srcVal">The source data returned by the server. For example '86399.123456789'</param>
        /// <returns>The corresponding number of ticks for the given value.</returns>
        private static long GetTicksFromSecondAndNanosecond(UTF8Buffer srcVal)
        {
            var intPart = 0L;
            var decimalPart = 0L;
            int dotIndex = Array.IndexOf<byte>(srcVal.Buffer, (byte)'.', srcVal.offset, srcVal.length);
            if (dotIndex == -1)
            {
                intPart = FastParser.FastParseInt64(srcVal.Buffer, srcVal.offset, srcVal.length);
            }
            else
            {
                dotIndex -= srcVal.offset;
                intPart = FastParser.FastParseInt64(srcVal.Buffer, srcVal.offset, dotIndex);
                var decimalPartLength = srcVal.length - dotIndex - 1;
                decimalPart = FastParser.FastParseInt64(srcVal.Buffer, srcVal.offset + dotIndex + 1, decimalPartLength);
                // If the decimal part contained less than nine characters, we must convert the value to nanoseconds by
                // multiplying by 10^[precision difference].
                if (decimalPartLength < 9 && decimalPartLength > 0)
                {
                    decimalPart *= powersOf10[9 - decimalPartLength];
                }
            }

            return intPart * 10000000L + decimalPart / 100L;

        }

        internal static Tuple<string, string> csharpTypeValToSfTypeVal(DbType srcType, object srcVal)
        {
            SFDataType destType;
            string destVal;

            switch (srcType)
            {
                case DbType.Decimal:
                case DbType.SByte:
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.Byte:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                case DbType.VarNumeric:
                    destType = SFDataType.FIXED;
                    break;

                case DbType.Boolean:
                    destType = SFDataType.BOOLEAN;
                    break;

                case DbType.Double:
                case DbType.Single:
                    destType = SFDataType.REAL;
                    break;

                case DbType.Guid:
                case DbType.String:
                case DbType.StringFixedLength:
                    destType = SFDataType.TEXT;
                    break;

                case DbType.Date:
                    destType = SFDataType.DATE;
                    break;

                case DbType.Time:
                    destType = SFDataType.TIME;
                    break;

                case DbType.DateTime:
                case DbType.DateTime2:
                    destType = SFDataType.TIMESTAMP_NTZ;
                    break;

                // By default map DateTimeoffset to TIMESTAMP_TZ
                case DbType.DateTimeOffset:
                    destType = SFDataType.TIMESTAMP_TZ;
                    break;

                case DbType.Binary:
                    destType = SFDataType.BINARY;
                    break;

                default:
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_DOTNET_TYPE, srcType);
            }
            destVal = csharpValToSfVal(destType, srcVal);
            return Tuple.Create(destType.ToString(), destVal);
        }

        private static string BytesToHex(byte[] bytes)
        {
            StringBuilder hexBuilder = new StringBuilder(bytes.Length * 2);
            foreach(byte b in bytes)
            {
                hexBuilder.AppendFormat("{0:x2}", b);
            }
            return hexBuilder.ToString();
        }

        private static byte[] HexToBytes(string hex)
        {
            int NumberChars = hex.Length;
            byte[] bytes = new byte[NumberChars / 2];
            for (int i = 0; i < NumberChars; i += 2)
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            return bytes;
        }

        internal static string csharpValToSfVal(SFDataType sfDataType, object srcVal)
        {
            string destVal = null;

            if (srcVal != DBNull.Value)
            {
                switch (sfDataType)
                {
                    case SFDataType.TIMESTAMP_LTZ:
                        if (srcVal.GetType() != typeof(DateTimeOffset))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), SFDataType.TIMESTAMP_LTZ.ToString());
                        }
                        else
                        {
                            destVal = ((long)(((DateTimeOffset)srcVal).UtcTicks - UnixEpoch.Ticks) * 100).ToString();
                        }
                        break;

                    case SFDataType.FIXED:
                    case SFDataType.BOOLEAN:
                    case SFDataType.REAL:
                    case SFDataType.TEXT:
                        destVal = string.Format(CultureInfo.InvariantCulture, "{0}", srcVal);
                        break;

                    case SFDataType.TIME:
                        if (srcVal.GetType() != typeof(DateTime))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), DbType.Time.ToString());
                        }
                        else
                        {
                            DateTime srcDt = ((DateTime)srcVal);
                            long nanoSinceMidNight = (long)(srcDt.Ticks - srcDt.Date.Ticks) * 100L;

                            destVal = nanoSinceMidNight.ToString();
                        }
                        break;

                    case SFDataType.DATE:
                        if (srcVal.GetType() != typeof(DateTime))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), DbType.Date.ToString());
                        }
                        else
                        {
                            DateTime dt = ((DateTime)srcVal).Date;
                            var ts = dt.Subtract(UnixEpoch);
                            long millis = (long)(ts.TotalMilliseconds);
                            destVal = millis.ToString();
                        }
                        break;

                    case SFDataType.TIMESTAMP_NTZ:
                        if (srcVal.GetType() != typeof(DateTime))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), DbType.DateTime.ToString());
                        }
                        else
                        {
                            DateTime srcDt = (DateTime)srcVal;
                            var diff = srcDt.Subtract(UnixEpoch);
                            var tickDiff = diff.Ticks;
                            destVal = $"{tickDiff}00"; // Cannot multiple tickDiff by 100 because long might overflow.
                        }
                        break;

                    case SFDataType.TIMESTAMP_TZ:
                        if (srcVal.GetType() != typeof(DateTimeOffset))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), DbType.DateTimeOffset.ToString());
                        }
                        else
                        {
                            DateTimeOffset dtOffset = (DateTimeOffset)srcVal;
                            destVal = String.Format("{0} {1}", (dtOffset.UtcTicks - UnixEpoch.Ticks) * 100L,
                                dtOffset.Offset.TotalMinutes + 1440);
                        }
                        break;

                    case SFDataType.BINARY:
                        if (srcVal.GetType() != typeof(byte[]))
                        {
                            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal,
                                srcVal.GetType().ToString(), DbType.Binary.ToString());
                        }
                        else
                        {
                            destVal = BytesToHex((byte[])srcVal);
                        }
                        break;

                    default:
                        throw new SnowflakeDbException(
                            SFError.UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM, sfDataType.ToString());
                }
            }
            return destVal;
        }

        internal static string toDateString(DateTime date, string formatter)
        {
            // change formatter from "YYYY-MM-DD" to "yyyy-MM-dd"
            formatter = formatter.Replace("Y", "y").Replace("m", "M").Replace("D", "d");
            return date.ToString(formatter);
        }
    }
}
