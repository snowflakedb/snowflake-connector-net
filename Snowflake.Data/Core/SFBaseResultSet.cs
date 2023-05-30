/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{

    abstract class SFBaseResultSet
    {
        internal SFStatement sfStatement;

        internal SFResultSetMetaData sfResultSetMetaData;

        internal int columnCount;

        internal bool isClosed;
        
        internal string queryId;

        internal abstract bool Next();

        internal abstract Task<bool> NextAsync();

        internal abstract bool NextResult();

        internal abstract Task<bool> NextResultAsync(CancellationToken cancellationToken);

        internal abstract bool HasRows();

        internal abstract UTF8Buffer getObjectInternal(int columnIndex);

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal abstract bool Rewind();

        protected SFBaseResultSet()
        {
        }

        internal T GetValue<T>(int columnIndex)
        {
            UTF8Buffer val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            return (T)SFDataConverter.ConvertToCSharpVal(val, types.Item1, typeof(T));
        }

        internal string GetString(int columnIndex)
        {
            var type = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            switch (type)
            {
                case SFDataType.DATE:
                    var val = GetValue(columnIndex);
                    if (val == DBNull.Value)
                        return null;
                    return SFDataConverter.toDateString((DateTime)val, 
                        sfResultSetMetaData.dateOutputFormat);
                //TODO: Implement SqlFormat for timestamp type, aka parsing format specified by user and format the value
                default:
                    return getObjectInternal(columnIndex).SafeToString(); 
            }
        }

        internal object GetValue(int columnIndex)
        {
            UTF8Buffer val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            return SFDataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2);
        }

        internal bool IsDBNull(int ordinal)
        {
            return (null == getObjectInternal(ordinal));
        }

        internal void close()
        {
            isClosed = true;
        }
        
    }
}
