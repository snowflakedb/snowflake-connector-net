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
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Linq;

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
                Console.WriteLine("GetObject rowType.name: " + rowType.name);
                var fields = rowType.fields;
                if (fields == null || fields.Count == 0 || !JsonToStructuredTypeConverter.IsObjectType(rowType.type))
                {
                    throw new StructuredTypesReadingException($"Method GetObject<{typeof(T)}> can be used only for structured object");
                }

                if (ResultFormat == ResultFormat.JSON)
                {
                    var stringValue = GetString(ordinal);
                    Console.WriteLine("GetObject stringValue: " + stringValue);
                    var json = stringValue == null ? null : JObject.Parse(stringValue);
                    Console.WriteLine("GetObject json: " + json);
                    return JsonToStructuredTypeConverter.ConvertObject<T>(fields, json);
                }
                else
                {
                    var val = resultSet.GetValue(0);
                    var obj = ArrowConverter.FormatStructArray((StructArray)val, 0);
                    return ArrowConverter.ToObject<T>(obj);
                }
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
                Console.WriteLine("GetArray rowType.name: " + rowType.name);
                var fields = rowType.fields;
                var isArrayOrVector = JsonToStructuredTypeConverter.IsArrayType(rowType.type) ||
                                      JsonToStructuredTypeConverter.IsVectorType(rowType.type);
                if (fields == null || fields.Count == 0 || !isArrayOrVector)
                {
                    throw new StructuredTypesReadingException($"Method GetArray<{typeof(T)}> can be used only for structured array or vector types");
                }

                var stringValue = GetString(ordinal);
                Console.WriteLine("GetArray stringValue: " + stringValue);
                var json = stringValue == null ? null : JArray.Parse(stringValue);
                Console.WriteLine("GetArray json: " + json);
                return JsonToStructuredTypeConverter.ConvertArray<T>(fields, json);
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
                Console.WriteLine("GetMap rowType.name: " + rowType.name);
                var fields = rowType.fields;
                if (fields == null || fields.Count == 0 || !JsonToStructuredTypeConverter.IsMapType(rowType.type))
                {
                    throw new StructuredTypesReadingException($"Method GetMap<{typeof(TKey)}, {typeof(TValue)}> can be used only for structured map");
                }

                var stringValue = GetString(ordinal);
                Console.WriteLine("GetMap stringValue: " + stringValue);
                var json = stringValue == null ? null : JObject.Parse(stringValue);
                Console.WriteLine("GetMap json: " + json);
                return JsonToStructuredTypeConverter.ConvertMap<TKey, TValue>(fields, json);
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

namespace Snowflake.Data.Core.Converter
{
    internal static class ArrowConverter
    {
        internal static T ToObject<T>(this Dictionary<string, object> dict) where T : new()
        {
            Console.WriteLine($"ToObject dict.Count: {dict.Count}");

            T obj = new T();
            Type type = typeof(T);

            foreach (var kvp in dict)
            {
                var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase |  BindingFlags.Public | BindingFlags.Instance);

                Console.WriteLine($"ToObject kvp.Key: {kvp.Key}");
                Console.WriteLine($"ToObject kvp.Value: {kvp.Value}");
                Console.WriteLine($"ToObject kvp.Type: {kvp.Value.GetType().Name}");
                Console.WriteLine($"ToObject prop: {prop}");

                if (prop != null)
                {
                    var value = kvp.Value;

                    if (value is List<object> objList)
                    {
                        if (prop.PropertyType.IsArray)
                        {
                            var stringArray = objList.Select(o => o?.ToString()).ToArray();
                            prop.SetValue(obj, stringArray);
                        }
                        else if (prop.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                        {
                            var strList = objList.OfType<string>().ToList();
                            prop.SetValue(obj, strList);
                        }
                    }
                    else if (value is Dictionary<object, object> objDict)
                    {
                        var stringDict = objDict.ToDictionary(
                            kv => kv.Key?.ToString(),
                            kv => kv.Value?.ToString()
                        );
                        Console.WriteLine($"ToObject stringDict: {stringDict}");
                        prop.SetValue(obj, stringDict);
                    }
                    else
                    {
                        //value = Convert.ChangeType(value, prop.PropertyType);
                        prop.SetValue(obj, value);
                    }
                }
            }

            return obj;
        }

        internal static object FormatArrowValue(IArrowArray array, int index)
        {
            //Console.WriteLine($"FormatArrowValue array.GetType().Name: {array.GetType().Name}");
            switch (array)
            {
                case StructArray strct: return FormatStructArray(strct, index);
                case MapArray map: return FormatArrowMapArray(map, index);
                case ListArray list: return FormatArrowListArray(list, index);
                case DoubleArray doubles: return doubles.GetValue(index);
                case FloatArray floats: return floats.GetValue(index);
                case Decimal128Array decimals: return decimals.GetValue(index);
                case Int32Array ints: return ints.GetValue(index);
                case Int64Array longs: return longs.GetValue(index);
                case StringArray strArray:
                    var str = strArray.GetString(index);
                    return string.IsNullOrEmpty(str) ? null : str;
                default:
                    throw new NotSupportedException($"Unsupported array type: {array.GetType()}");
            }
        }

        internal static Dictionary<string, object> FormatStructArray(StructArray structArray, int index)
        {
            var result = new Dictionary<string, object>();
            var structTypeFields = ((StructType)structArray.Data.DataType).Fields;

            for (int i = 0; i < structArray.Fields.Count; i++)
            {
                var field = structArray.Fields[i];
                var fieldName = structTypeFields[i].Name;
                var value = FormatArrowValue(field, index);

                if (value == null && structArray.Fields.Count == 1)
                    return null;

                result[fieldName] = value;
            }

            return result;
        }

        internal static List<object> FormatArrowListArray(ListArray listArray, int index)
        {
            int start = listArray.ValueOffsets[index];
            int end = listArray.ValueOffsets[index + 1];

            if (start == end)
                return null;

            var values = listArray.Values;
            var result = new List<object>(end - start);

            for (int i = start; i < end; i++)
            {
                result.Add(FormatArrowValue(values, i));
            }

            return result;
        }

        internal static Dictionary<object, object> FormatArrowMapArray(MapArray mapArray, int index)
        {
            int start = mapArray.ValueOffsets[index];
            int end = mapArray.ValueOffsets[index + 1];

            if (start == end)
                return null;

            var keyValuesArray = mapArray.KeyValues.Slice(start, end - start) as StructArray;
            var keyArray = keyValuesArray.Fields[0];
            var valueArray = keyValuesArray.Fields[1];

            var result = new Dictionary<object, object>();

            for (int i = 0; i < end - start; i++)
            {
                var key = FormatArrowValue(keyArray, i);
                var value = FormatArrowValue(valueArray, i);
                result[key] = value;
            }

            return result;
        }
    }
}
