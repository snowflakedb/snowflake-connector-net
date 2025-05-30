using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{

    abstract class SFBaseResultSet
    {
        internal abstract ResultFormat ResultFormat { get; }

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

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal abstract bool Rewind();

        protected SFBaseResultSet()
        {
        }

        internal abstract bool IsDBNull(int ordinal);

        internal abstract object GetValue(int ordinal);

        internal abstract bool GetBoolean(int ordinal);

        internal abstract byte GetByte(int ordinal);

        internal abstract long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length);

        internal abstract char GetChar(int ordinal);

        internal abstract long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length);

        internal abstract DateTime GetDateTime(int ordinal);

        internal abstract TimeSpan GetTimeSpan(int ordinal);

        internal abstract decimal GetDecimal(int ordinal);

        internal abstract double GetDouble(int ordinal);

        internal abstract float GetFloat(int ordinal);

        internal abstract Guid GetGuid(int ordinal);

        internal abstract short GetInt16(int ordinal);

        internal abstract int GetInt32(int ordinal);

        internal abstract long GetInt64(int ordinal);

        internal abstract string GetString(int ordinal);

        internal void close()
        {
            isClosed = true;
        }

        internal void ThrowIfClosed()
        {
            if (isClosed)
                throw new SnowflakeDbException(SFError.DATA_READER_ALREADY_CLOSED);
        }

        internal void ThrowIfOutOfBounds(int ordinal)
        {
            if (ordinal < 0 || ordinal >= columnCount)
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, ordinal);
        }

    }
}
