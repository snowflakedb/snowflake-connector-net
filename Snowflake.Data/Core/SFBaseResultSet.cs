/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
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

        private SFDataConverter dataConverter;

        protected SFBaseResultSet()
        {
            dataConverter = new SFDataConverter();
        }

        internal T GetValue<T>(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            return (T) dataConverter.ConvertToCSharpVal(val, types.Item1, typeof(T));
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
                    return getObjectInternal(columnIndex); 
            }
        }

        internal object GetValue(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            return dataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2);
        }
        
        internal void close()
        {
            isClosed = true;
        }

        internal abstract int CalculateUpdateCount();

    }
}
