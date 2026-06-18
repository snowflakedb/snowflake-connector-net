using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Diagnostics;

namespace Snowflake.Data.Core
{
    class SFResultSet : SFBaseResultSet
    {
        internal override ResultFormat ResultFormat => ResultFormat.JSON;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFResultSet>();

        private readonly int _totalChunkCount;

        private readonly IChunkDownloader _chunkDownloader;

        private BaseResultChunk _currentChunk;

        public SFResultSet(QueryExecResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            try
            {
                columnCount = responseData.rowType?.Count ?? 0;

                this.sfStatement = sfStatement;
                UpdateSessionStatus(responseData);

                if (responseData.chunks != null)
                {
                    // counting the first chunk
                    _totalChunkCount = responseData.chunks.Count;
                    _chunkDownloader = ChunkDownloaderFactory.GetDownloader(responseData, this, cancellationToken);
                }

                _currentChunk = responseData.rowSet != null ? new SFResultChunk(responseData.rowSet) : null;
                responseData.rowSet = null;

                sfResultSetMetaData = responseData.rowType != null ? new SFResultSetMetaData(responseData, this.sfStatement.SfSession) : null;

                isClosed = false;

                queryId = responseData.queryId;
            }
            catch (System.Exception ex)
            {
                s_logger.Error("Result set error queryId=" + responseData.queryId, ex);
                throw;
            }
        }

        public enum PutGetResponseRowTypeInfo
        {
            SourceFileName = 0,
            DestinationFileName = 1,
            SourceFileSize = 2,
            DestinationFileSize = 3,
            SourceCompressionType = 4,
            DestinationCompressionType = 5,
            ResultStatus = 6,
            ErrorDetails = 7
        }

        public void InitializePutGetRowType(List<ExecResponseRowType> rowType)
        {
            foreach (PutGetResponseRowTypeInfo t in System.Enum.GetValues(typeof(PutGetResponseRowTypeInfo)))
            {
                rowType.Add(new ExecResponseRowType()
                {
                    name = t.ToString(),
                    type = "text"
                });
            }
        }

        public SFResultSet(PutGetResponseData responseData, SFStatement sfStatement, CancellationToken cancellationToken) : base()
        {
            responseData.rowType = new List<ExecResponseRowType>();
            InitializePutGetRowType(responseData.rowType);

            columnCount = responseData.rowType.Count;

            this.sfStatement = sfStatement;

            _currentChunk = new SFResultChunk(responseData.rowSet);
            responseData.rowSet = null;

            sfResultSetMetaData = new SFResultSetMetaData(responseData);

            isClosed = false;

            queryId = responseData.queryId;
        }

        internal void ResetChunkInfo(BaseResultChunk nextChunk)
        {
            s_logger.Debug($"Received chunk #{nextChunk.ChunkIndex + 1} of {_totalChunkCount}");
            _currentChunk.RowSet = null;
            _currentChunk = nextChunk;
        }

        internal override async Task<bool> NextAsync()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_chunkDownloader != null)
            {
                // GetNextChunk could be blocked if download result is not done yet. 
                // So put this piece of code in a seperate task
                s_logger.Debug($"Get next chunk from chunk downloader, chunk: {_currentChunk.ChunkIndex + 1}/{_totalChunkCount}" +
                               $" rows: {_currentChunk.RowCount}, size compressed: {_currentChunk.CompressedSize}," +
                               $" size uncompressed: {_currentChunk.UncompressedSize}");
                BaseResultChunk nextChunk = await _chunkDownloader.GetNextChunkAsync().ConfigureAwait(false);
                if (nextChunk != null)
                {
                    ResetChunkInfo(nextChunk);
                    return _currentChunk.Next();
                }
            }

            return false;
        }

        internal override bool Next()
        {
            ThrowIfClosed();

            if (_currentChunk.Next())
                return true;

            if (_chunkDownloader != null)
            {
                s_logger.Debug($"Get next chunk from chunk downloader, chunk: {_currentChunk.ChunkIndex + 1}/{_totalChunkCount}" +
                               $" rows: {_currentChunk.RowCount}, size compressed: {_currentChunk.CompressedSize}," +
                               $" size uncompressed: {_currentChunk.UncompressedSize}");
                BaseResultChunk nextChunk = Task.Run(async () => await (_chunkDownloader.GetNextChunkAsync()).ConfigureAwait(false)).Result;
                if (nextChunk != null)
                {
                    ResetChunkInfo(nextChunk);
                    return _currentChunk.Next();
                }
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
            ThrowIfClosed();

            return _currentChunk.RowCount > 0 || _totalChunkCount > 0;
        }

        /// <summary>
        /// Move cursor back one row.
        /// </summary>
        /// <returns>True if it works, false otherwise.</returns>
        internal override bool Rewind()
        {
            ThrowIfClosed();

            return _currentChunk.Rewind();
        }

        internal UTF8Buffer GetObjectInternal(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            return _currentChunk.ExtractCell(ordinal);
        }

        private void UpdateSessionStatus(QueryExecResponseData responseData)
        {
            SFSession session = this.sfStatement.SfSession;
            session.UpdateSessionProperties(responseData);
            session.UpdateSessionParameterMap(responseData.parameters);
            session.UpdateQueryContextCache(responseData.QueryContext);
        }

        internal override bool IsDBNull(int ordinal)
        {
            return (null == GetObjectInternal(ordinal));
        }

        internal override bool GetBoolean(int ordinal)
        {
            return GetValue<bool>(ordinal);
        }

        internal override byte GetByte(int ordinal)
        {
            return GetValue<byte>(ordinal);
        }

        internal override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return ReadSubset<byte>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override char GetChar(int ordinal)
        {
            string val = GetString(ordinal);
            return val[0];
        }

        internal override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return ReadSubset<char>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        internal override DateTime GetDateTime(int ordinal)
        {
            return GetValue<DateTime>(ordinal);
        }

        internal override TimeSpan GetTimeSpan(int ordinal)
        {
            return GetValue<TimeSpan>(ordinal);
        }

        internal override decimal GetDecimal(int ordinal)
        {
            return GetValue<decimal>(ordinal);
        }

        internal override double GetDouble(int ordinal)
        {
            return GetValue<double>(ordinal);
        }

        internal override float GetFloat(int ordinal)
        {
            return GetValue<float>(ordinal);
        }

        internal override Guid GetGuid(int ordinal)
        {
            return GetValue<Guid>(ordinal);
        }

        internal override short GetInt16(int ordinal)
        {
            return GetValue<short>(ordinal);
        }

        internal override int GetInt32(int ordinal)
        {
            return GetValue<int>(ordinal);
        }

        internal override long GetInt64(int ordinal)
        {
            return GetValue<long>(ordinal);
        }

        internal override string GetString(int ordinal)
        {
            ThrowIfOutOfBounds(ordinal);

            var type = sfResultSetMetaData.GetColumnTypeByIndex(ordinal);
            switch (type)
            {
                case SFDataType.DATE:
                    var val = GetValue(ordinal);
                    if (val == DBNull.Value)
                        return null;
                    return SFDataConverter.ToDateString((DateTime)val, sfResultSetMetaData.dateOutputFormat);

                default:
                    return GetObjectInternal(ordinal).SafeToString();
            }
        }

        internal override object GetValue(int ordinal)
        {
            UTF8Buffer val = GetObjectInternal(ordinal);
            var types = sfResultSetMetaData.GetTypesByIndex(ordinal);
            var sessionTimezone = sfStatement.SfSession.GetSessionTimezone();
            return SFDataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2, sessionTimezone);
        }

        private T GetValue<T>(int ordinal)
        {
            UTF8Buffer val = GetObjectInternal(ordinal);
            var types = sfResultSetMetaData.GetTypesByIndex(ordinal);
            var sessionTimezone = sfStatement.SfSession.GetSessionTimezone();
            return (T)SFDataConverter.ConvertToCSharpVal(val, types.Item1, typeof(T), sessionTimezone);
        }

        //
        // Summary:
        //     Reads a subset of data starting at location indicated by dataOffset into the buffer,
        //     starting at the location indicated by bufferOffset.
        //
        // Parameters:
        //   ordinal:
        //     The zero-based column ordinal.
        //
        //   dataOffset:
        //     The index within the data from which to begin the read operation.
        //
        //   buffer:
        //     The buffer into which to copy the data.
        //
        //   bufferOffset:
        //     The index with the buffer to which the data will be copied.
        //
        //   length:
        //     The maximum number of elements to read.
        //
        // Returns:
        //     The actual number of elements read.
        private long ReadSubset<T>(int ordinal, long dataOffset, T[] buffer, int bufferOffset, int length) where T : struct
        {
            if (dataOffset < 0)
            {
                throw new ArgumentOutOfRangeException("dataOffset", "Non negative number is required.");
            }

            if (bufferOffset < 0)
            {
                throw new ArgumentOutOfRangeException("bufferOffset", "Non negative number is required.");
            }

            if ((null != buffer) && (bufferOffset > buffer.Length))
            {
                throw new System.ArgumentException("Destination buffer is not long enough. " +
                    "Check the buffer offset, length, and the buffer's lower bounds.", "buffer");
            }

            T[] data = GetValue<T[]>(ordinal);

            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getbytes?view=net-5.0#remarks
            // If you pass a buffer that is null, GetBytes returns the length of the row in bytes.
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getchars?view=net-5.0#remarks
            // If you pass a buffer that is null, GetChars returns the length of the field in characters.
            if (null == buffer)
            {
                return data.Length;
            }

            if (dataOffset > data.Length)
            {
                throw new System.ArgumentException("Source data is not long enough. " +
                    "Check the data offset, length, and the data's lower bounds.", "dataOffset");
            }
            else
            {
                // How much data is available after the offset
                long dataLength = data.Length - dataOffset;
                // How much data to read
                long elementsRead = Math.Min(length, dataLength);
                Array.Copy(data, dataOffset, buffer, bufferOffset, elementsRead);

                return elementsRead;
            }
        }
    }
}
