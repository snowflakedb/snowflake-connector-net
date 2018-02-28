/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    abstract class SFBaseResultSet
    {
        internal SFStatement sfStatement;

        internal SFResultSetMetaData sfResultSetMetaData;

        internal int columnCount;

        internal bool isClosed;

        internal abstract bool Next();
        internal abstract Task<bool> NextAsync();

        protected abstract string getObjectInternal(int columnIndex);

        internal byte[] getBytes(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (byte[])SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(byte[]));
        }

        internal DateTime getDateTime(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (DateTime)SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(DateTime));
        }

        internal Decimal getDecimal(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (Decimal)SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Decimal));
        }
        internal double getDouble(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (double)SFDataConverter.convertToCSharpVal(val, sfDataType ,typeof(double));
        }
        internal float getFloat(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (float)SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(float));
        }
        internal Guid getGuid(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (Guid)SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Guid));
        }

        internal string getString(int columnIndex)
        {
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);

            switch(sfDataType)
            {
                case SFDataType.DATE:
                    return SFDataConverter.toDateString(getDateTime(columnIndex), 
                        sfResultSetMetaData.dateOutputFormat);
                default:
                    return getObjectInternal(columnIndex);
            }
        }

        internal short getInt16(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (Int16) SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Int16));
        }

        internal int getInt32(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (Int32) SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Int32));
        }

        internal long getInt64(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (Int64) SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Int64));
        }

        internal bool getBoolean(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (bool)SFDataConverter.convertToCSharpVal(val, sfDataType, typeof(Boolean));
        }

        internal DateTimeOffset getDateTimeOffset(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            SFDataType sfDataType = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            return (DateTimeOffset) SFDataConverter.convertToCSharpVal(
                        val, sfDataType, typeof(DateTimeOffset));
        }

        internal object getValue(int columnIndex)
        {
            Type type = sfResultSetMetaData.getCSharpTypeByIndex(columnIndex);
            if (type == typeof(long))
            {
                return getInt64(columnIndex);
            }
            else if (type == typeof(decimal))
            {
                return getDecimal(columnIndex);
            }
            else if (type == typeof(string))
            {
                return getString(columnIndex);
            }
            else if (type == typeof(byte))
            {
                return getBytes(columnIndex);
            }
            else if (type == typeof(double))
            {
                return getDouble(columnIndex);
            }
            else if (type == typeof(DateTime))
            {
                return getDateTime(columnIndex);
            }
            else if (type == typeof(DateTimeOffset))
            {
                return getDateTimeOffset(columnIndex);
            }
            else if (type == typeof(bool))
            {
                return getBoolean(columnIndex);
            }
            else
            { 
                throw new NotImplementedException();
            }
        }
        
        internal void close()
        {
            isClosed = true;
        }
        
    }
}
