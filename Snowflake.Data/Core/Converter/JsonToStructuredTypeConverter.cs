using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter
{
    internal static class JsonToStructuredTypeConverter
    {
        private static readonly TimeConverter s_timeConverter = new TimeConverter();

        public static T ConvertObject<T>(string sourceTypeName, List<FieldMetadata> fields, JObject value)
        {
            var type = typeof(T);
            if (SFDataType.OBJECT.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (T) ConvertToObject(type, fields, value);
            }

            throw new Exception("OBJECT type was expected");
        }

        public static T[] ConvertArray<T>(string sourceTypeName, List<FieldMetadata> fields, JArray value)
        {
            var type = typeof(T[]);
            var elementType = typeof(T);
            if (SFDataType.ARRAY.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (T[]) ConvertToArray(type, elementType, fields, value);
            }

            throw new Exception("ARRAY type was expected");
        }

        public static Dictionary<TKey, TValue> ConvertMap<TKey, TValue>(string sourceTypeName, List<FieldMetadata> fields, JObject value)
        {
            var type = typeof(Dictionary<TKey, TValue>);
            var keyType = typeof(TKey);
            var valueType = typeof(TValue);
            if (SFDataType.MAP.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (Dictionary<TKey, TValue>) ConvertToMap(type, keyType, valueType, fields, value);
            }

            throw new Exception("MAP type was expected");
        }


        private static object ConvertToObject(Type type, List<FieldMetadata> fields, JToken json)
        {
            var context = $"converting structured object to client type {type}";
                if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
                {
                    return null;
                }

                if (json.Type != JTokenType.Object)
                {
                    throw new Exception("Json is not an object");
                }

                var jsonObject = (JObject)json;
                var constructionMethod = GetConstructionMethod(type);
                var objectBuilder = ObjectBuilderFactory.Create(type, fields?.Count ?? 0, constructionMethod);
                context = $"converting structured object fields to client type {type}";
                using (var metadataIterator = fields.GetEnumerator())
                {
                    using (var jsonEnumerator = jsonObject.GetEnumerator())
                    {
                        do
                        {
                            var nextMetadataAvailable = metadataIterator.MoveNext();
                            var nextJsonAvailable = jsonEnumerator.MoveNext();
                            if (nextMetadataAvailable ^ nextJsonAvailable) // exclusive or
                            {
                                throw new Exception("Internal error: object fields count not matching metadata fields count");
                            }

                            if (!nextMetadataAvailable)
                                break;
                            var jsonPropertyWithValue = jsonEnumerator.Current;
                            var fieldMetadata = metadataIterator.Current;
                            var key = jsonPropertyWithValue.Key;
                            var fieldValue = jsonPropertyWithValue.Value;
                            try
                            {
                                var fieldType = objectBuilder.MoveNext(key);
                                var value = ConvertToStructuredOrUnstructuredValue(fieldType, fieldMetadata, fieldValue);
                                objectBuilder.BuildPart(value);
                            }
                            catch (Exception e)
                            {
                                if (e is SnowflakeDbException)
                                    throw;
                                throw new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_ERROR, $"when handling {fieldMetadata.name} property", e.Message);
                            }
                        } while (true);
                    }
                }
                return objectBuilder.Build();
        }

        private static SnowflakeObjectConstructionMethod GetConstructionMethod(Type type)
        {
            return type.GetCustomAttributes(false)
                .Where(attribute => attribute.GetType() == typeof(SnowflakeObject))
                .Select(attribute => ((SnowflakeObject)attribute).ConstructionMethod)
                .FirstOrDefault();
        }

        private static object ConvertToUnstructuredType(FieldMetadata fieldMetadata, Type fieldType, JToken json)
        {
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (IsTextMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(char) || fieldType == typeof(char?))
                {
                    return char.Parse(value);
                }
                if (fieldType == typeof(Guid) || fieldType == typeof(Guid?))
                {
                    return Guid.Parse(value);
                }
                return value;
            }
            if (IsFixedMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(byte) || fieldType == typeof(byte?))
                {
                    return byte.Parse(value);
                }
                if (fieldType == typeof(sbyte) || fieldType == typeof(sbyte?))
                {
                    return sbyte.Parse(value);
                }
                if (fieldType == typeof(short) || fieldType == typeof(short?))
                {
                    return short.Parse(value);
                }
                if (fieldType == typeof(ushort) || fieldType == typeof(ushort?))
                {
                    return ushort.Parse(value);
                }
                if (fieldType == typeof(Int32) || fieldType == typeof(Int32?))
                {
                    var bytes = Encoding.UTF8.GetBytes(value);
                    return FastParser.FastParseInt32(bytes, 0, bytes.Length);
                }
                if (fieldType == typeof(Int64) || fieldType == typeof(Int64?))
                {
                    var bytes = Encoding.UTF8.GetBytes(value);
                    return FastParser.FastParseInt64(bytes, 0, bytes.Length);
                }
                throw new Exception($"Could not read {fieldMetadata.type} into {fieldType}");
            }
            if (IsRealMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                var bytes = Encoding.UTF8.GetBytes(value);
                var decimalValue = FastParser.FastParseDecimal(bytes, 0, bytes.Length);
                if (fieldType == typeof(float) || fieldType == typeof(float?))
                {
                    return (float) decimalValue;
                }
                if (fieldType == typeof(double) || fieldType == typeof(double?))
                {
                    return (double) decimalValue;
                }
                return decimalValue;
            }
            if (IsBooleanMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                var bytes = Encoding.UTF8.GetBytes(value);
                return SFDataConverter.ConvertToBooleanValue(new UTF8Buffer(bytes));
            }
            if (IsTimestampNtz(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFTimestampType.TIMESTAMP_NTZ, fieldType);
            }
            if (IsTimestampLtz(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFTimestampType.TIMESTAMP_LTZ, fieldType);
            }
            if (IsTimestampTz(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFTimestampType.TIMESTAMP_TZ, fieldType);
            }
            if (IsTime(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFTimestampType.TIME, fieldType);
            }
            if (IsDate(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFTimestampType.DATE, fieldType);
            }
            if (IsBinaryMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(byte[]))
                {
                    return SFDataConverter.HexToBytes(value);
                }
                throw new Exception($"Could not read BINARY into {fieldType}");
            }
            if (IsObjectMetadata(fieldMetadata)) // semi structured object
            {
                return json.ToString();
            }
            if (IsArrayMetadata(fieldMetadata)) // semi structured array
            {
                return json.ToString();
            }
            if (IsVariantMetadata(fieldMetadata))
            {
                return json.ToString();
            }
            throw new Exception($"Could not read {fieldMetadata.type} into {fieldType}");
        }

        private static object ConvertToArray(Type type, Type elementType, List<FieldMetadata> fields, JToken json)
        {
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Array)
            {
                throw new Exception("Json is not an array");
            }
            var jsonArray = (JArray)json;
            var arrayType = MakeArrayType(type, elementType);
            var result = (IList) Activator.CreateInstance(arrayType, jsonArray.Count);
            var elementMetadata = fields[0];
            for (var i = 0; i < jsonArray.Count; i++)
            {
                result[i] = ConvertToStructuredOrUnstructuredValue(elementType, elementMetadata, jsonArray[i]);
            }
            if (type != arrayType)
            {
                var listType = type.IsAbstract ? typeof(List<>).MakeGenericType(elementType) : type;
                var list = (IList) Activator.CreateInstance(listType);
                for (int i = 0; i < result.Count; i++)
                {
                    list.Add(result[i]);
                }
                return list;
            }

            return result;
        }

        private static object ConvertToMap(Type type, Type keyType, Type valueType, List<FieldMetadata> fields, JToken json)
        {
            if (keyType != typeof(string)
                && keyType != typeof(int) && keyType != typeof(int?)
                && keyType != typeof(long) && keyType != typeof(long?))
            {
                throw new Exception($"Unsupported key type of dictionary {keyType}");
            }
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Object)
            {
                throw new Exception("Json is not an object");
            }
            if (fields == null || fields.Count != 2)
            {
                throw new Exception("Expected map to have 2 metadata fields");
            }
            var keyMetadata = fields[0];
            var fieldMetadata = fields[1];
            var jsonObject = (JObject)json;
            var dictionaryType = type.IsAbstract ? typeof(Dictionary<,>).MakeGenericType(keyType, valueType) : type;
            var result = (IDictionary) Activator.CreateInstance(dictionaryType);
            using (var jsonEnumerator = jsonObject.GetEnumerator())
            {
                while (jsonEnumerator.MoveNext())
                {
                    var jsonPropertyWithValue = jsonEnumerator.Current;
                    var fieldValue = jsonPropertyWithValue.Value;
                    var key = IsTextMetadata(keyMetadata) || IsFixedMetadata(keyMetadata)
                        ? ConvertToUnstructuredType(keyMetadata, keyType, jsonPropertyWithValue.Key)
                        : throw new Exception("Could not recognize key type for map");
                    var value = ConvertToStructuredOrUnstructuredValue(valueType, fieldMetadata, fieldValue);
                    result.Add(key, value);
                }
            }
            return result;
        }

        private static object ConvertToStructuredOrUnstructuredValue(Type valueType, FieldMetadata fieldMetadata, JToken fieldValue)
        {
            try
            {
                if (IsObjectMetadata(fieldMetadata) && IsStructured(fieldMetadata))
                {
                    return ConvertToObject(valueType, fieldMetadata.fields, fieldValue);
                }

                if (IsArrayMetadata(fieldMetadata) && IsStructured(fieldMetadata))
                {
                    var nestedType = GetNestedType(valueType);
                    return ConvertToArray(valueType, nestedType, fieldMetadata.fields, fieldValue);
                }

                if (IsMapMetadata(fieldMetadata) && IsStructured(fieldMetadata))
                {
                    var keyValueTypes = GetMapKeyValueTypes(valueType);
                    return ConvertToMap(valueType, keyValueTypes[0], keyValueTypes[1], fieldMetadata.fields, fieldValue);
                }

                return ConvertToUnstructuredType(fieldMetadata, valueType, fieldValue);
            }
            catch (Exception e)
            {
                if (e is SnowflakeDbException)
                    throw;
                throw new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_ERROR, $"when reading '{fieldMetadata.name}' property", e.Message);
            }
        }

        private static Type GetNestedType(Type type)
        {
            if (type.IsArray)
            {
                return type.GetElementType();
            }
            if (IsListType(type) || IsIListType(type))
            {
                return type.GenericTypeArguments[0];
            }
            throw new Exception("neither array nor list");
        }

        private static Type[] GetMapKeyValueTypes(Type type)
        {
            var genericParamWithTwoArguments = type.IsGenericType && type.GenericTypeArguments.Length == 2;
            if (!genericParamWithTwoArguments)
            {
                throw new Exception("Could not get key and value types");
            }
            return type.GenericTypeArguments;
        }

        private static Type MakeArrayType(Type type, Type elementType)
        {
            if (type.IsArray)
            {
                return type;
            }
            if (IsListType(type) || IsIListType(type))
            {
                return elementType.MakeArrayType();
            }
            throw new Exception("Neither array nor list");
        }

        private static bool IsListType(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);

        private static bool IsIListType(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>);

        private static bool IsStructured(FieldMetadata fieldMetadata) =>
            fieldMetadata.fields != null && fieldMetadata.fields.Count > 0;

        private static bool IsObjectMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.OBJECT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTextMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TEXT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsFixedMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.FIXED.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampNtz(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_NTZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampLtz(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_LTZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampTz(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_TZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTime(FieldMetadata fieldMetadata) =>
            SFDataType.TIME.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsDate(FieldMetadata fieldMetadata) =>
            SFDataType.DATE.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsRealMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.REAL.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsBooleanMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.BOOLEAN.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsBinaryMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.BINARY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsArrayMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.ARRAY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsVariantMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.VARIANT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsMapMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.MAP.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);
    }
}
