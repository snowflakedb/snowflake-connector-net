using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    class ResultSetUtil
    {
        internal static int calculateUpdateCount(SFBaseResultSet resultSet)
        {
            SFResultSetMetaData metaData = resultSet.sfResultSetMetaData;
            SFStatementType statementType = metaData.statementType;

            int updateCount = 0;    
            switch(statementType)
            {
                case SFStatementType.INSERT:
                case SFStatementType.UPDATE:
                case SFStatementType.DELETE:
                case SFStatementType.MERGE:
                case SFStatementType.MULTI_INSERT:
                    for(int i=0; i<resultSet.columnCount; i++)
                    {
                        updateCount += resultSet.getInt32(i);
                    }
                    break;
                case SFStatementType.COPY:
                    updateCount = resultSet.getInt32(3);
                    break;
                default:
                    updateCount = 0;
                    break;
            }

            return updateCount;
        } 
    }
}
