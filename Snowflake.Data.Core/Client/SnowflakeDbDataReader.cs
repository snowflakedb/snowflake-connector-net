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

        private DataTable SchemaTable;

        private int RecordsAffectedInternal;

        internal ResultFormat ResultFormat => resultSet.ResultFormat;
        
        internal SnowflakeDbDataReader(SnowflakeDbCommand command, SFBaseResultSet resultSet)
        {
            this.dbCommand = command;
            this.resultSet = resultSet;
            this.isClosed = false;
            this.SchemaTable = PopulateSchemaTable(resultSet);
            RecordsAffectedInternal = resultSet.CalculateUpdateCount();
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
                return resultSet.HasResultSet() && resultSet.HasRows();
            }
        }

        public override bool IsClosed
        {
            get
            {
                return this.isClosed;
            }
        }

        public override int RecordsAffected { get { return RecordsAffectedInternal; } }

        public override DataTable GetSchemaTable()
        {
            return this.SchemaTable;
        }

        public string GetQueryId()
        {
            return resultSet.queryId;
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
            return resultSet.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return resultSet.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return resultSet.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return resultSet.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return resultSet.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            resultSet.ThrowIfOutOfBounds(ordinal);
            return resultSet.sfResultSetMetaData.GetColumnTypeByIndex(ordinal).ToString();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return resultSet.GetDateTime(ordinal);
        }

        public TimeSpan GetTimeSpan(int ordinal)
        {
            return resultSet.GetTimeSpan(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return resultSet.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return resultSet.GetDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            resultSet.ThrowIfOutOfBounds(ordinal);
            return resultSet.sfResultSetMetaData.GetCSharpTypeByIndex(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return resultSet.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return resultSet.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return resultSet.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return resultSet.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return resultSet.GetInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            resultSet.ThrowIfOutOfBounds(ordinal);
            return resultSet.sfResultSetMetaData.GetColumnNameByIndex(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return resultSet.sfResultSetMetaData.GetColumnIndexByName(name);
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
            if (resultSet.NextResult())
            {
                this.SchemaTable = PopulateSchemaTable(resultSet);
                RecordsAffectedInternal = resultSet.CalculateUpdateCount();
                return true;
            }
            return false;
        }

        public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            if (await resultSet.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
                this.SchemaTable = PopulateSchemaTable(resultSet);
                RecordsAffectedInternal = resultSet.CalculateUpdateCount();
                return true;
            }
            return false;
        }

        public override bool Read()
        {
            return resultSet.Next();
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return await resultSet.NextAsync();
        }

        public override void Close()
        {
            base.Close();
            resultSet.close();
            isClosed = true;
        }

    }
}
