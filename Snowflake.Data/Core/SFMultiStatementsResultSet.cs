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
        internal override ResultFormat ResultFormat => curResultSet.ResultFormat;

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
                if (!await NextResultAsync(CancellationToken.None))
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

        private void updateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.UpdateSessionProperties(responseData);
            session.UpdateSessionParameterMap(responseData.parameters);
            session.UpdateQueryContextCache(responseData.QueryContext);
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

        internal override bool IsDBNull(int ordinal)
        {
            return curResultSet.IsDBNull(ordinal);
        }

        internal override object GetValue(int ordinal)
        {
            return curResultSet.GetValue(ordinal);
        }

        internal override bool GetBoolean(int ordinal)
        {
            return curResultSet.GetBoolean(ordinal);
        }

        internal override byte GetByte(int ordinal)
        {
            return curResultSet.GetByte(ordinal);
        }

        internal override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return curResultSet.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override char GetChar(int ordinal)
        {
            return curResultSet.GetChar(ordinal);
        }

        internal override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return curResultSet.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override DateTime GetDateTime(int ordinal)
        {
            return curResultSet.GetDateTime(ordinal);
        }

        internal override TimeSpan GetTimeSpan(int ordinal)
        {
            return curResultSet.GetTimeSpan(ordinal);
        }

        internal override decimal GetDecimal(int ordinal)
        {
            return curResultSet.GetDecimal(ordinal);
        }

        internal override double GetDouble(int ordinal)
        {
            return curResultSet.GetDouble(ordinal);
        }

        internal override float GetFloat(int ordinal)
        {
            return curResultSet.GetFloat(ordinal);
        }

        internal override Guid GetGuid(int ordinal)
        {
            return curResultSet.GetGuid(ordinal);
        }

        internal override short GetInt16(int ordinal)
        {
            return curResultSet.GetInt16(ordinal);
        }

        internal override int GetInt32(int ordinal)
        {
            return curResultSet.GetInt32(ordinal);
        }

        internal override long GetInt64(int ordinal)
        {
            return curResultSet.GetInt64(ordinal);
        }

        internal override string GetString(int ordinal)
        {
            return curResultSet.GetString(ordinal);
        }
    }
}
