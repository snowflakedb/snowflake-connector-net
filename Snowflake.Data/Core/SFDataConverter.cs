﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using System.Threading;
using Snowflake.Data.Log;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    public enum SFDataType
    {
        None, FIXED, REAL, TEXT, DATE, VARIANT, TIMESTAMP_LTZ, TIMESTAMP_NTZ,
        TIMESTAMP_TZ, OBJECT, BINARY, TIME, BOOLEAN, ARRAY
    }

    class SFDataConverter
    {
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFDataConverter>();

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private readonly Dictionary<Type, TypeConverter> typeConverters = new Dictionary<Type, TypeConverter>();

        internal SFDataConverter()
        {
        }

        private static long ParseInt64(string s)
        {
            return long.Parse(s, CultureInfo.InvariantCulture);
        }

        private static string Int64ToString(long v)
        {
            return v.ToString(CultureInfo.InvariantCulture);
        }

        private static int ParseInt32(string s)
        {
            return int.Parse(s, CultureInfo.InvariantCulture);
        }

        internal object ConvertToCSharpVal(string srcVal, SFDataType srcType, Type destType)
        {
            //Logger.DebugFmt("src value: {0}, srcType: {1}, destType: {2}", srcVal, srcType, destType);

            if (srcVal == null)
                return DBNull.Value;

            if (destType == typeof(short) 
                || destType == typeof(int)
                || destType == typeof(long)
                || destType == typeof(Guid)
                || destType == typeof(double)
                || destType == typeof(float)
                || destType == typeof(decimal))
            {
                if (!typeConverters.ContainsKey(destType))
                {
                    typeConverters.Add(destType, TypeDescriptor.GetConverter(destType));
                }
                return typeConverters[destType].ConvertFromInvariantString(srcVal);
            }
            else if (destType == typeof(Boolean))
            {
                return srcVal.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                       srcVal.Equals("true", StringComparison.OrdinalIgnoreCase);
            }
            else if (destType == typeof(string))
            {
                return srcVal;
            }
            else if (destType == typeof(DateTime))
            {
                return ConvertToDateTime(srcVal, srcType);
            }
            else if (destType == typeof(DateTimeOffset))
            {
                return ConvertToDateTimeOffset(srcVal, srcType);
            }
            else if (destType == typeof(byte[]))
            {
                return srcType == SFDataType.BINARY ? 
                    HexToBytes(srcVal) : Encoding.UTF8.GetBytes(srcVal);
            }
            else
            {
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Invalid destination type.");
            }
        }

        private static DateTime ConvertToDateTime(string srcVal, SFDataType srcType)
        {
            switch (srcType)
            {
                case SFDataType.DATE:
                    long srcValLong = ParseInt64(srcVal);
                    return UnixEpoch.AddDays(srcValLong);

                case SFDataType.TIME:
                case SFDataType.TIMESTAMP_NTZ:

                    Tuple<long, long> secAndNsec = ExtractTimestamp(srcVal);
                    var tickDiff = secAndNsec.Item1 * 10000000L + secAndNsec.Item2 / 100L;
                    return UnixEpoch.AddTicks(tickDiff);

                default:
                    throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, srcType, typeof(DateTime));
            }
        }

        private static DateTimeOffset ConvertToDateTimeOffset(string srcVal, SFDataType srcType)
        {
            switch (srcType)
            {
                case SFDataType.TIMESTAMP_TZ:
                    int spaceIndex = srcVal.IndexOf(' ');
                    if (spaceIndex == -1)
                    {
                        throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                            $"Invalid timestamp_tz value: {srcVal}");
                    }
                    else
                    {
                        Tuple<long, long> secAndNsecTZ = ExtractTimestamp(srcVal.Substring(0, spaceIndex));

                        int offset = ParseInt32(srcVal.Substring(spaceIndex + 1, srcVal.Length - spaceIndex - 1));
                        TimeSpan offSetTimespan = new TimeSpan((offset - 1440) / 60, 0, 0);
                        return new DateTimeOffset(UnixEpoch.Ticks +
                            (secAndNsecTZ.Item1 * 1000 * 1000 * 1000 + secAndNsecTZ.Item2) / 100, TimeSpan.Zero).ToOffset(offSetTimespan);
                    }
                case SFDataType.TIMESTAMP_LTZ:
                    Tuple<long, long> secAndNsecLTZ = ExtractTimestamp(srcVal);
                      
                    return new DateTimeOffset(UnixEpoch.Ticks +
                        (secAndNsecLTZ.Item1 * 1000 * 1000 * 1000 + secAndNsecLTZ.Item2) / 100, TimeSpan.Zero).ToLocalTime(); 

                default:
                    throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, 
                        srcType, typeof(DateTimeOffset).ToString());
            }
        }

        private static Tuple<long, long> ExtractTimestamp(string srcVal)
        {
            int dotIndex = srcVal.IndexOf('.');
            if (dotIndex == -1)
            {
                return Tuple.Create(ParseInt64(srcVal), (long)0);
            }
            else
            {
                var intPart = ParseInt64(srcVal.Substring(0, dotIndex));
                var decimalPartLength = srcVal.Length - dotIndex - 1;
                var decimalPartStr = srcVal.Substring(dotIndex + 1, decimalPartLength);
                var decimalPart = ParseInt64(decimalPartStr);
                // If the decimal part contained less than nine characters, we must convert the value to nanoseconds by
                // multiplying by 10^[precision difference].
                if (decimalPartLength < 9)
                {
                    decimalPart *= (int) Math.Pow(10, 9 - decimalPartLength);
                }

                return Tuple.Create(intPart, decimalPart);                
            }
        }

            
        internal static Tuple<string, string> csharpTypeValToSfTypeVal(DbType srcType, object srcVal)
        {
            string destType;
            string destVal;
            var srcValAsCultureInvariantString = (srcVal == DBNull.Value) ? null : string.Format(CultureInfo.InvariantCulture, "{0}", srcVal);

            switch (srcType)
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
                    destVal = srcValAsCultureInvariantString;
                    break;

                case DbType.Boolean:
                    destType = SFDataType.BOOLEAN.ToString();
                    destVal = srcValAsCultureInvariantString;
                    break;

                case DbType.Double:
                    destType = SFDataType.REAL.ToString();
                    destVal = srcValAsCultureInvariantString;
                    break;

                case DbType.Guid:
                case DbType.String:
                case DbType.StringFixedLength:
                    destType = SFDataType.TEXT.ToString();
                    destVal = srcValAsCultureInvariantString;
                    break;

                case DbType.Date:
                    destType = SFDataType.DATE.ToString();
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
                        destVal = Int64ToString(millis);
                    }
                    break;

                case DbType.Time:
                    destType = SFDataType.TIME.ToString();
                    if (srcVal.GetType() != typeof(DateTime))
                    {
                        throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, 
                            srcVal.GetType().ToString(), DbType.Time.ToString());
                    }
                    else
                    {
                        DateTime srcDt = ((DateTime)srcVal);
                        long nanoSinceMidNight = (long)(srcDt.Ticks - srcDt.Date.Ticks) * 100L; 

                        destVal = Int64ToString(nanoSinceMidNight);
                    }
                    break;

                case DbType.DateTime:
                    destType = SFDataType.TIMESTAMP_NTZ.ToString();
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
                
                // by default map DateTimeoffset to TIMESTAMP_TZ
                case DbType.DateTimeOffset:
                    destType = SFDataType.TIMESTAMP_TZ.ToString();
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

                case DbType.Binary:
                    destType = SFDataType.BINARY.ToString();
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
                    throw new NotImplementedException();
            }
            return Tuple.Create(destType, destVal);
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
            switch(sfDataType)
            {
                case SFDataType.TIMESTAMP_LTZ:
                    if (srcVal.GetType() != typeof(DateTimeOffset))
                    {
                        throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, srcVal, 
                            srcVal.GetType().ToString(), SFDataType.TIMESTAMP_LTZ.ToString());
                    }
                    else
                    {
                        return Int64ToString((long)(((DateTimeOffset)srcVal).UtcTicks - UnixEpoch.Ticks) * 100);
                    }
                default:
                    throw new NotImplementedException();
            }
        }

        internal static string toDateString(DateTime date, string formatter)
        {
            // change formatter from "YYYY-MM-DD" to "yyyy-MM-dd"
            formatter = formatter.Replace("Y", "y").Replace("m", "M").Replace("D", "d");
            return date.ToString(formatter);
        }
    }
}
