using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class ArrowResultSet : SFBaseResultSet
    {
        internal override ResultFormat ResultFormat => ResultFormat.ARROW;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ArrowResultSet>();

        private readonly int _totalChunkCount;
        private BaseResultChunk _currentChunk;
        private readonly IChunkDownloader _chunkDownloader;

        public ArrowResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken)
        {
            columnCount = responseData.rowType.Count;
            try
            {
                this.sfStatement = sfStatement;
                UpdateSessionStatus(responseData);

                if (responseData.chunks != null)
                {
                    _totalChunkCount = responseData.chunks.Count;
                    _chunkDownloader = ChunkDownloaderFactory.GetDownloader(responseData, this, cancellationToken);
                }

                responseData.rowSet = null;

                sfResultSetMetaData = new SFResultSetMetaData(responseData, this.sfStatement.SfSession);

                isClosed = false;

                queryId = responseData.queryId;

                ReadChunk(responseData);
            }
            catch (Exception ex)
            {
                s_logger.Error("Result set error queryId=" + responseData.queryId, ex);
                throw;
            }
        }

        private void ReadChunk(QueryExecResponseData responseData)
        {
            if (responseData.rowsetBase64?.Length > 0)
            {
                using (var stream = new MemoryStream(Convert.FromBase64String(responseData.rowsetBase64)))
                {
                    using (var reader = new ArrowStreamReader(stream))
                    {
                        var recordBatch = reader.ReadNextRecordBatch();
                        _currentChunk = new ArrowResultChunk(recordBatch);
                        while ((recordBatch = reader.ReadNextRecordBatch()) != null)
                        {
                            ((ArrowResultChunk)_currentChunk).AddRecordBatch(recordBatch);
                        }
                    }
                }
            }
            else
            {
                _currentChunk = new ArrowResultChunk(columnCount);
            }
        }

        internal override async Task<bool> NextAsync()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_totalChunkCount > 0)
            {
                s_logger.Debug($"Get next chunk from chunk downloader, chunk: {_currentChunk.ChunkIndex + 1}/{_totalChunkCount}" +
                               $" rows: {_currentChunk.RowCount}, size compressed: {_currentChunk.CompressedSize}," +
                               $" size uncompressed: {_currentChunk.UncompressedSize}");
                _currentChunk = await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false);
                return _currentChunk?.Next() ?? false;
            }

            return false;
        }

        internal override bool Next()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_totalChunkCount > 0)
            {
                s_logger.Debug($"Get next chunk from chunk downloader, chunk: {_currentChunk.ChunkIndex + 1}/{_totalChunkCount}" +
                               $" rows: {_currentChunk.RowCount}, size compressed: {_currentChunk.CompressedSize}," +
                               $" size uncompressed: {_currentChunk.UncompressedSize}");
                _currentChunk = Task.Run(async () => await (_chunkDownloader.GetNextChunkAsync()).ConfigureAwait(false)).Result;

                return _currentChunk?.Next() ?? false;
            }

            return false;
        }

        internal override bool NextResult()
        {
            return false;
        }

        internal override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return await Task.FromResult(false);
        }

        internal override bool HasRows()
        {
            if (isClosed)
            {
                return false;
            }

            return _currentChunk.RowCount > 0 || _totalChunkCount > 0;
        }

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            ThrowIfClosed();

            if (_currentChunk.Rewind())
                return true;

            if (_currentChunk.ChunkIndex > 0)
            {
                s_logger.Warn("Unable to rewind to the previous chunk");
            }

            return false;
        }

        private object GetObjectInternal(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            var type = sfResultSetMetaData.GetTypesByIndex(ordinal).Item1;
            var scale = sfResultSetMetaData.GetScaleByIndex(ordinal);

            var value = ((ArrowResultChunk)_currentChunk).ExtractCell(ordinal, type, (int)scale);

            return value ?? DBNull.Value;

        }

        internal override object GetValue(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            if (value == DBNull.Value)
            {
                return value;
            }

            object obj;
            checked
            {
                switch (value)
                {
                    case decimal ret:
                        obj = ret;
                        break;
                    case long ret:
                        obj = ret;
                        break;
                    case int ret:
                        obj = (long)ret;
                        break;
                    case short ret:
                        obj = (long)ret;
                        break;
                    case sbyte ret:
                        obj = (long)ret;
                        break;
                    case string ret:
                        obj = ret;
                        break;
                    case bool ret:
                        obj = ret;
                        break;
                    case DateTime ret:
                        obj = ret;
                        break;
                    case DateTimeOffset ret:
                        obj = ret;
                        break;
                    default:
                        {
                            var dstType = sfResultSetMetaData.GetCSharpTypeByIndex(ordinal);
                            obj = Convert.ChangeType(value, dstType);
                            break;
                        }
                }
            }

            return obj;
        }

        internal override bool IsDBNull(int ordinal)
        {
            return GetObjectInternal(ordinal) == DBNull.Value;
        }

        internal override bool GetBoolean(int ordinal)
        {
            return (bool)GetObjectInternal(ordinal);
        }

        internal override byte GetByte(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            checked
            {
                switch (value)
                {
                    case decimal ret: return (byte)ret;
                    case long ret: return (byte)ret;
                    case int ret: return (byte)ret;
                    case short ret: return (byte)ret;
                    case sbyte ret: return (byte)ret;
                    default: return (byte)value;
                }
            }
        }

        internal override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return ReadSubset<byte>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override char GetChar(int ordinal)
        {
            return ((string)GetObjectInternal(ordinal))[0];
        }

        internal override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return ReadSubset<char>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override DateTime GetDateTime(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            if (value == DBNull.Value)
                return (DateTime)value;

            switch (value)
            {
                case DateTime ret:
                    return ret;
                case DateTimeOffset ret:
                    return ret.DateTime;
            }
            return (DateTime)Convert.ChangeType(value, typeof(DateTime));
        }

        internal override TimeSpan GetTimeSpan(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            if (value == DBNull.Value)
                return (TimeSpan)value;
            var type = sfResultSetMetaData.GetColumnTypeByIndex(ordinal);
            if (type == SFDataType.TIME && value is DateTime ret)
                return TimeSpan.FromTicks(ret.Ticks - SFDataConverter.UnixEpoch.Ticks);
            throw new SnowflakeDbException(SFError.INVALID_DATA_CONVERSION, value, type, typeof(TimeSpan));
        }

        internal override decimal GetDecimal(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            switch (value)
            {
                case double ret: return (decimal)ret;
                case float ret: return (decimal)ret;
                case long ret: return ret;
                case int ret: return ret;
                case short ret: return ret;
                case sbyte ret: return ret;
                default: return Convert.ToDecimal(value);
            }
        }

        internal override double GetDouble(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            switch (value)
            {
                case float ret: return ret;
                case decimal ret: return (double)ret;
                case long ret: return ret;
                case int ret: return ret;
                case short ret: return ret;
                case sbyte ret: return ret;
                default: return Convert.ToDouble(value);
            }
        }

        internal override float GetFloat(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            switch (value)
            {
                case double ret: return (float)ret;
                case decimal ret: return (float)ret;
                case long ret: return ret;
                case int ret: return ret;
                case short ret: return ret;
                case sbyte ret: return ret;
                default: return Convert.ToSingle(value);
            }
        }

        internal override Guid GetGuid(int ordinal)
        {
            return new Guid(GetString(ordinal));
        }

        internal override short GetInt16(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            checked
            {
                switch (value)
                {
                    case decimal ret: return (short)ret;
                    case long ret: return (short)ret;
                    case int ret: return (short)ret;
                    case sbyte ret: return ret;
                    default: return Convert.ToInt16(value);
                }
            }
        }

        internal override int GetInt32(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            checked
            {
                switch (value)
                {
                    case decimal ret: return (int)ret;
                    case long ret: return (int)ret;
                    case short ret: return ret;
                    case sbyte ret: return ret;
                    default: return Convert.ToInt32(value);
                }
            }
        }

        internal override long GetInt64(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            checked
            {
                switch (value)
                {
                    case decimal ret: return (long)ret;
                    case int ret: return ret;
                    case short ret: return ret;
                    case sbyte ret: return ret;
                    default: return Convert.ToInt64(value);
                }
            }
        }

        internal override string GetString(int ordinal)
        {
            var value = GetObjectInternal(ordinal);
            if (value == DBNull.Value)
                return (string)value;

            var type = sfResultSetMetaData.GetColumnTypeByIndex(ordinal);
            switch (value)
            {
                case string ret:
                    return ret;
                case DateTime ret:
                    if (type == SFDataType.DATE)
                        return SFDataConverter.ToDateString(ret, sfResultSetMetaData.dateOutputFormat);
                    break;
            }

            return Convert.ToString(value);
        }

        private void UpdateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.UpdateSessionProperties(responseData);
            session.UpdateSessionParameterMap(responseData.parameters);
        }

        private long ReadSubset<T>(int ordinal, long dataOffset, T[] buffer, int bufferOffset, int length) where T : struct
        {
            if (dataOffset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(dataOffset), "Non negative number is required.");
            }

            if (bufferOffset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferOffset), "Non negative number is required.");
            }

            if (buffer != null && bufferOffset > buffer.Length)
            {
                throw new System.ArgumentException(
                    "Destination buffer is not long enough. Check the buffer offset, length, and the buffer's lower bounds.",
                    nameof(buffer));
            }

            var value = GetObjectInternal(ordinal);
            var type = sfResultSetMetaData.GetColumnTypeByIndex(ordinal);
            Array data;
            if (type == SFDataType.BINARY)
                data = (byte[])value;
            else if (typeof(T) == typeof(byte))
                data = Encoding.ASCII.GetBytes(value.ToString());
            else
                data = value.ToString().ToCharArray();

            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getbytes?view=net-5.0#remarks
            // If you pass a buffer that is null, GetBytes returns the length of the row in bytes.
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getchars?view=net-5.0#remarks
            // If you pass a buffer that is null, GetChars returns the length of the field in characters.
            if (buffer == null)
            {
                return data.Length;
            }

            if (dataOffset > data.Length)
            {
                throw new System.ArgumentException(
                    "Source data is not long enough. Check the data offset, length, and the data's lower bounds.",
                    nameof(dataOffset));
            }

            long dataLength = data.Length - dataOffset;
            long elementsRead = Math.Min(length, dataLength);
            Array.Copy(data, dataOffset, buffer, bufferOffset, elementsRead);

            return elementsRead;

        }

    }
}
