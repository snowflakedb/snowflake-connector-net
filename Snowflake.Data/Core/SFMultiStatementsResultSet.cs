/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;

namespace Snowflake.Data.Core
{
    class SFMultiStatementsResultSet : SFBaseResultSet
    {
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFMultiStatementsResultSet>();

        private string[] resultIds;

        private SFBaseResultSet curResultSet;

        private int curResultIndex = -1;


        public SFMultiStatementsResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            this.sfStatement = sfStatement;
            this.resultIds = responseData.resultIds.Split(',');
            updateSessionStatus(responseData);
            queryId = responseData.queryId;

            NextResult();
        }

        internal override async Task<bool> NextAsync()
        {
            if (curResultSet == null)
            {
                if (! await NextResultAsync(CancellationToken.None))
                {
                    return false;
                }
            }
            return await curResultSet.NextAsync();
        }

        internal override bool Next()
        {
            if (curResultSet == null)
            {
                if (!NextResult())
                {
                    return false;
                }
            }
            return curResultSet.Next();
        }

        internal override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            if (++curResultIndex < resultIds.Length)
            {
                curResultSet = await sfStatement.GetResultWithIdAsync(
                                        resultIds[curResultIndex],
                                        cancellationToken);
            }
            else
            {
                curResultSet = null;
            }

            updateResultMetadata();
            return await Task.FromResult(curResultSet != null);
        }

        internal override bool NextResult()
        {
            if (++curResultIndex < resultIds.Length)
            {
                curResultSet = sfStatement.GetResultWithId(resultIds[curResultIndex]);
            }
            else
            {
                curResultSet = null;
            }

            updateResultMetadata();
            return (curResultSet != null);
        }

        internal override bool HasRows()
        {
            if (curResultSet == null)
            {
                return false;
            }
            
            return curResultSet.HasRows();
        }
        
        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            return curResultSet.Rewind();
        }

        internal override UTF8Buffer getObjectInternal(int columnIndex)
        {
            return curResultSet.getObjectInternal(columnIndex);
        }

        private void updateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.database = responseData.finalDatabaseName;
            session.schema = responseData.finalSchemaName;

            session.UpdateSessionParameterMap(responseData.parameters);
        }

        private void updateResultMetadata()
        {
            if (curResultSet != null)
            {
                isClosed = curResultSet.isClosed;
                columnCount = curResultSet.columnCount;
                sfResultSetMetaData = curResultSet.sfResultSetMetaData;
            }
            else
            {
                isClosed = true;
                columnCount = 0;
                sfResultSetMetaData = null;
            }
        }
    }
}
