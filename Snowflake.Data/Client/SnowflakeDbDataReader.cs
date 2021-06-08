/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using System.Collections;
using Snowflake.Data.Core;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using System.Text;
using System.IO;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbDataReader : DbDataReader
    {
        static private readonly SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbDataReader>();

        private SnowflakeDbCommand dbCommand;

        private SFBaseResultSet resultSet;

        private bool isClosed;

        private readonly DataTable SchemaTable;

        internal SnowflakeDbDataReader(SnowflakeDbCommand command, SFBaseResultSet resultSet)
        {
            this.dbCommand = command;
            this.resultSet = resultSet;
            this.isClosed = false;
            this.SchemaTable = PopulateSchemaTable(resultSet);
            RecordsAffected = resultSet.CalculateUpdateCount();
        }

        public override object this[string name]
        {
            get
            {
                return resultSet.GetValue(GetOrdinal(name));
            }
        }

        public override object this[int ordinal]
        {
            get
            {
                return resultSet.GetValue(ordinal);
            }
        }

        public override int Depth
        {
            get
            {
                return 0;
            }
        }

        public override int FieldCount
        {
            get
            {
                return resultSet.columnCount;
            }
        }

        public override bool HasRows
        {
            get
            {
                // return true for now since every query returned from server
                // will have at least one row
                return true;
            }
        }

        public override bool IsClosed
        {
            get
            {
                return this.isClosed;
            }
        }

        public override int RecordsAffected { get; }

        public override DataTable GetSchemaTable()
        {
            return this.SchemaTable;
        }

        private DataTable PopulateSchemaTable(SFBaseResultSet resultSet)
        {
            var table = new DataTable("SchemaTable");

            table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
            table.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(int));
            table.Columns.Add(SchemaTableColumn.NumericScale, typeof(int));
            table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
            table.Columns.Add(SchemaTableColumn.ProviderType, typeof(SFDataType));

            int columnOrdinal = 0;
            SFResultSetMetaData sfResultSetMetaData = resultSet.sfResultSetMetaData;
            foreach (ExecResponseRowType rowType in sfResultSetMetaData.rowTypes)
            {
                var row = table.NewRow();

                row[SchemaTableColumn.ColumnName] = rowType.name;
                row[SchemaTableColumn.ColumnOrdinal] = columnOrdinal;
                row[SchemaTableColumn.ColumnSize] = (int)rowType.length;
                row[SchemaTableColumn.NumericPrecision] = (int)rowType.precision;
                row[SchemaTableColumn.NumericScale] = (int)rowType.scale;
                row[SchemaTableColumn.AllowDBNull] = rowType.nullable;

                Tuple<SFDataType, Type> types = sfResultSetMetaData.GetTypesByIndex(columnOrdinal);
                row[SchemaTableColumn.ProviderType] = types.Item1;
                row[SchemaTableColumn.DataType] = types.Item2;

                table.Rows.Add(row);
                columnOrdinal++;
            }

            return table;
        }
		
        public override bool GetBoolean(int ordinal)
        {
            return resultSet.GetValue<bool>(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            byte[] bytes = resultSet.GetValue<byte[]>(ordinal);
            return bytes[0];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return readSubset<byte>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            string val = resultSet.GetString(ordinal);
            return val[0];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return readSubset<char>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return resultSet.sfResultSetMetaData.getColumnTypeByIndex(ordinal).ToString();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return resultSet.GetValue<DateTime>(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return resultSet.GetValue<decimal>(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return resultSet.GetValue<double>(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            return resultSet.sfResultSetMetaData.getCSharpTypeByIndex(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return resultSet.GetValue<float>(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return resultSet.GetValue<Guid>(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return resultSet.GetValue<short>(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return resultSet.GetValue<int>(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return resultSet.GetValue<long>(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return resultSet.sfResultSetMetaData.getColumnNameByIndex(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return resultSet.sfResultSetMetaData.getColumnIndexByName(name);
        }

        public override string GetString(int ordinal)
        {
            return resultSet.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return resultSet.GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            int count = Math.Min(FieldCount, values.Length);
            for (int i=0; i< count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return resultSet.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            return resultSet.Next();
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new TaskCanceledException();

            return resultSet.NextAsync();
        }

        public override void Close()
        {
            base.Close();
            resultSet.close();
            isClosed = true;
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
        private long readSubset<T>(int ordinal, long dataOffset, T[] buffer, int bufferOffset, int length)
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

            T[] data = resultSet.GetValue<T[]>(ordinal);

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
                    "Check the data offset, length, and the data's lower bounds." ,"dataOffset");
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
