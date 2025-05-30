using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    internal static class ResultSetUtil
    {
        internal static int CalculateUpdateCount(this SFBaseResultSet resultSet)
        {
            SFResultSetMetaData metaData = resultSet.sfResultSetMetaData;
            SFStatementType statementType = metaData.statementType;

            long updateCount = 0;
            switch (statementType)
            {
                case SFStatementType.INSERT:
                case SFStatementType.UPDATE:
                case SFStatementType.DELETE:
                case SFStatementType.MERGE:
                case SFStatementType.MULTI_INSERT:
                    resultSet.Next();
                    for (int i = 0; i < resultSet.columnCount; i++)
                    {
                        updateCount += resultSet.GetInt64(i);
                    }
                    resultSet.Rewind();
                    break;
                case SFStatementType.COPY:
                    var index = resultSet.sfResultSetMetaData.GetColumnIndexByName("rows_loaded");
                    if (index >= 0)
                    {
                        while (resultSet.Next())
                        {
                            updateCount += resultSet.GetInt64(index);
                        }
                        while (resultSet.Rewind()) { }
                    }
                    break;
                case SFStatementType.COPY_UNLOAD:
                    var rowIndex = resultSet.sfResultSetMetaData.GetColumnIndexByName("rows_unloaded");
                    if (rowIndex >= 0)
                    {
                        resultSet.Next();
                        updateCount = resultSet.GetInt64(rowIndex);
                        resultSet.Rewind();
                    }
                    break;
                case SFStatementType.SELECT:
                    // DbDataReader.RecordsAffected returns -1 for SELECT statement
                    updateCount = -1;
                    break;
                default:
                    updateCount = 0;
                    break;
            }

            if (updateCount > int.MaxValue)
                return -1;

            return (int)updateCount;
        }

        internal static bool IsDQL(this SFBaseResultSet resultSet)
        {
            if (resultSet.isClosed) return false;

            SFResultSetMetaData metaData = resultSet.sfResultSetMetaData;
            SFStatementType statementType = metaData.statementType;

            switch (statementType)
            {
                case SFStatementType.SELECT:
                case SFStatementType.EXPLAIN:
                case SFStatementType.SHOW:
                case SFStatementType.DESCRIBE:
                case SFStatementType.LIST_FILES:
                case SFStatementType.GET_FILES:
                case SFStatementType.PUT_FILES:
                case SFStatementType.REMOVE_FILES:
                case SFStatementType.CALL:
                    return true;
                default:
                    return false;
            }
        }
    }
}
