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
    internal static class ResultSetUtil
    {
        internal static int CalculateUpdateCount(this SFBaseResultSet resultSet)
        {
            SFResultSetMetaData metaData = resultSet.sfResultSetMetaData;
            SFStatementType statementType = metaData.statementType;

            try
            {
                int updateCount = 0;
                switch (statementType)
                {
                    case SFStatementType.INSERT:
                    case SFStatementType.UPDATE:
                    case SFStatementType.DELETE:
                    case SFStatementType.MERGE:
                    case SFStatementType.MULTI_INSERT:
                        for (int i = 0; i < resultSet.columnCount; i++)
                        {
                            updateCount += resultSet.GetValue<int>(i);
                        }

                        break;
                    case SFStatementType.COPY:
                        var index = resultSet.sfResultSetMetaData.getColumnIndexByName("rows_loaded");
                        if (index >= 0) updateCount = resultSet.GetValue<int>(index);
                        break;
                    case SFStatementType.SELECT:
                        updateCount = -1;
                        break;
                    default:
                        updateCount = 0;
                        break;
                }

                return updateCount;
            }
            catch (Exception ex)
            {
                if (IsOverflowException(ex))
                    return -1;

                throw;
            }
        }

        private static bool IsOverflowException(Exception ex)
        {
            if (ex is OverflowException)
                return true;

            if (ex.InnerException != null)
                return IsOverflowException(ex.InnerException);

            if (ex is AggregateException aggEx)
                return aggEx.InnerExceptions.Any(IsOverflowException);

            return false;
        }
    }
}
