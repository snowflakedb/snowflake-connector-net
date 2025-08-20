using System;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using Snowflake.Data.Core;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Core.Converter;

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

        private const int MaxStringLength = 16777216; // Default maximum allowed length for VARCHAR

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
                return !resultSet.isClosed && resultSet.HasRows();
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
                row[SchemaTableColumn.ColumnSize] = IsStructuredOrSemiStructuredType(rowType.type) && rowType.length == 0 ? MaxStringLength : (int)rowType.length;
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

        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

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
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public T GetObject<T>(int ordinal)
            where T : class, new()
        {
            try
            {
                var rowType = resultSet.sfResultSetMetaData.rowTypes[ordinal];
                var fields = rowType.fields;
                if (fields == null || fields.Count == 0 || !JsonToStructuredTypeConverter.IsObjectType(rowType.type))
                {
                    throw new StructuredTypesReadingException($"Method GetObject<{typeof(T)}> can be used only for structured object");
                }
                var val = GetValue(ordinal);
                if (val is string stringValue)
                {
                    var json = stringValue == null ? null : JObject.Parse(stringValue);
                    return JsonToStructuredTypeConverter.ConvertObject<T>(fields, json);
                }
                if (val is Dictionary<string, object> structArray)
                    return ArrowConverter.ConvertObject<T>(structArray);
                return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("GetObject stack trace: " + e.StackTrace);
                Console.WriteLine("GetObject error: " + e.Message);
                if (e is SnowflakeDbException)
                    throw;
                throw StructuredTypesReadingHandler.ToSnowflakeDbException(e, "when getting an object");
            }
        }

        public T[] GetArray<T>(int ordinal)
        {
            try
            {
                var rowType = resultSet.sfResultSetMetaData.rowTypes[ordinal];
                var fields = rowType.fields;
                var isArrayOrVector = JsonToStructuredTypeConverter.IsArrayType(rowType.type) ||
                                      JsonToStructuredTypeConverter.IsVectorType(rowType.type);
                if (fields == null || fields.Count == 0 || !isArrayOrVector)
                {
                    throw new StructuredTypesReadingException($"Method GetArray<{typeof(T)}> can be used only for structured array or vector types");
                }
                var val = GetValue(ordinal);
                if (val is string stringValue)
                {
                    var json = stringValue == null ? null : JArray.Parse(stringValue);
                    return JsonToStructuredTypeConverter.ConvertArray<T>(fields, json);
                }
                if (val is List<object> listArray)
                    return ArrowConverter.ConvertArray<T>(listArray);
                return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("GetArray stack trace: " + e.StackTrace);
                Console.WriteLine("GetArray error: " + e.Message);
                if (e is SnowflakeDbException)
                    throw;
                throw StructuredTypesReadingHandler.ToSnowflakeDbException(e, "when getting an array");
            }
        }

        public Dictionary<TKey, TValue> GetMap<TKey, TValue>(int ordinal)
        {
            try
            {
                var rowType = resultSet.sfResultSetMetaData.rowTypes[ordinal];
                var fields = rowType.fields;
                if (fields == null || fields.Count == 0 || !JsonToStructuredTypeConverter.IsMapType(rowType.type))
                {
                    throw new StructuredTypesReadingException($"Method GetMap<{typeof(TKey)}, {typeof(TValue)}> can be used only for structured map");
                }
                var val = GetValue(ordinal);
                if (val is string stringValue)
                {
                    var json = stringValue == null ? null : JObject.Parse(stringValue);
                    return JsonToStructuredTypeConverter.ConvertMap<TKey, TValue>(fields, json);
                }
                if (val is Dictionary<object, object> mapArray)
                    return ArrowConverter.ConvertMap<TKey, TValue>(mapArray);
                return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("GetMap stack trace: " + e.StackTrace);
                Console.WriteLine("GetMap error: " + e.Message);
                if (e is SnowflakeDbException)
                    throw;
                throw StructuredTypesReadingHandler.ToSnowflakeDbException(e, "when getting a map");
            }
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

        private bool IsStructuredOrSemiStructuredType(string type)
        {
            type = type.ToLower();
            return type == "array" || type == "object" || type == "variant" || type == "map";
        }
    }
}
