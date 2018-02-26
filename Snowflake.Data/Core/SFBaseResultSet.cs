/*1/
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

        internal abstract bool next();

        protected abstract string getObjectInternal(int columnIndex);

        internal T GetValue<T>(int columnIndex)
        {
            return (T) GetValue(columnIndex);
        }

        internal string GetString(int columnIndex)
        {
            var type = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            switch (type)
            {
                case SFDataType.DATE:
                    var val = GetValue(columnIndex);
                    if (val == null)
                        return null;
                    return SFDataConverter.toDateString((DateTime)val, 
                        sfResultSetMetaData.dateOutputFormat);
                //TODO: Feels like were missing some implementations here, at least for time?
                default:
                    return getObjectInternal(columnIndex); 
            }
        }

        internal object GetValue(int columnIndex)
        {
            string val = getObjectInternal(columnIndex);
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            return SFDataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2);
        }
        
        internal void close()
        {
            isClosed = true;
        }
    }
}
