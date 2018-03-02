﻿/*
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
                case SFStatementType.SELECT:
                    updateCount = -1;
                    break;
                default:
                    updateCount = 0;
                    break;
            }

            return updateCount;
        } 
    }
}
