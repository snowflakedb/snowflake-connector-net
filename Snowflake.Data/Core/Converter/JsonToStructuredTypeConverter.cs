using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Converter.Builder;

namespace Snowflake.Data.Core.Converter
{
    internal static class JsonToStructuredTypeConverter
    {
        private static readonly TimeConverter s_timeConverter = new TimeConverter();

        public static T ConvertObject<T>(List<FieldMetadata> fields, JObject value, TimeZoneInfo sessionTimezone)
        {
            var type = typeof(T);
            return (T)ConvertToObject(type, fields, new StructurePath(), value, sessionTimezone);
        }

        public static T[] ConvertArray<T>(List<FieldMetadata> fields, JArray value, TimeZoneInfo sessionTimezone)
        {
            var type = typeof(T[]);
            var elementType = typeof(T);
            return (T[])ConvertToArray(type, elementType, fields, new StructurePath(), value, sessionTimezone);
        }

        public static Dictionary<TKey, TValue> ConvertMap<TKey, TValue>(List<FieldMetadata> fields, JObject value, TimeZoneInfo sessionTimezone)
        {
            var type = typeof(Dictionary<TKey, TValue>);
            var keyType = typeof(TKey);
            var valueType = typeof(TValue);
            return (Dictionary<TKey, TValue>)ConvertToMap(type, keyType, valueType, fields, new StructurePath(), value, sessionTimezone);
        }

        private static object ConvertToObject(Type type, List<FieldMetadata> fields, StructurePath structurePath, JToken json, TimeZoneInfo sessionTimezone)
        {
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }

            if (json.Type != JTokenType.Object)
            {
                throw new StructuredTypesReadingException($"JSON value is not a JSON object. Occured for path {structurePath}");
            }

            var jsonObject = (JObject)json;
            var constructionMethod = GetConstructionMethod(type);
            var objectBuilder = ObjectBuilderFactory.Create(type, fields?.Count ?? 0, constructionMethod);
            using (var metadataIterator = fields.GetEnumerator())
            {
                using (var jsonEnumerator = jsonObject.GetEnumerator())
                {
                    var propertyIndex = -1;
                    do
                    {
                        var nextMetadataAvailable = metadataIterator.MoveNext();
                        var nextJsonAvailable = jsonEnumerator.MoveNext();
                        if (nextMetadataAvailable ^ nextJsonAvailable) // exclusive or
                        {
                            throw new StructuredTypesReadingException($"Internal error: object fields count not matching metadata fields count. Occured for path {structurePath}");
                        }

                        if (!nextMetadataAvailable)
                            break;
                        propertyIndex++;
                        var propertyPath = structurePath.WithPropertyIndex(propertyIndex);
                        var jsonPropertyWithValue = jsonEnumerator.Current;
                        var fieldMetadata = metadataIterator.Current;
                        var key = jsonPropertyWithValue.Key;
                        var fieldValue = jsonPropertyWithValue.Value;
                        try
                        {
                            var fieldType = objectBuilder.MoveNext(key);
                            var value = ConvertToStructuredOrUnstructuredValue(fieldType, fieldMetadata, propertyPath, fieldValue, sessionTimezone);
                            objectBuilder.BuildPart(value);
                        }
                        catch (Exception e)
                        {
                            if (e is SnowflakeDbException)
                                throw;
                            throw StructuredTypesReadingHandler.ToSnowflakeDbException(e, $"when handling property with path {propertyPath}");
                        }
                    } while (true);
                }
            }
            return objectBuilder.Build();
        }

        internal static SnowflakeObjectConstructionMethod GetConstructionMethod(Type type)
        {
            return type.GetCustomAttributes(false)
                .Where(attribute => attribute.GetType() == typeof(SnowflakeObject))
                .Select(attribute => ((SnowflakeObject)attribute).ConstructionMethod)
                .FirstOrDefault();
        }

        private static object ConvertToUnstructuredType(FieldMetadata fieldMetadata, Type fieldType, JToken json, TimeZoneInfo sessionTimezone)
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
                if (fieldType == typeof(string))
                {
                    return value;
                }
                throw new StructuredTypesReadingException($"Could not read {fieldMetadata.type} type into {fieldType}");
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
                throw new StructuredTypesReadingException($"Could not read {fieldMetadata.type} type into {fieldType}");
            }
            if (IsRealMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                var bytes = Encoding.UTF8.GetBytes(value);
                if ((value.Contains("e") || value.Contains("E")))
                {
                    if (fieldType == typeof(float) || fieldType == typeof(float?))
                    {
                        return float.Parse(value);
                    }
                    else
                    {
                        return double.Parse(value);
                    }
                }
                var decimalValue = FastParser.FastParseDecimal(bytes, 0, bytes.Length);
                if (fieldType == typeof(float) || fieldType == typeof(float?))
                {
                    return (float)decimalValue;
                }
                if (fieldType == typeof(double) || fieldType == typeof(double?))
                {
                    return (double)decimalValue;
                }
                return decimalValue;
            }
            if (IsBooleanMetadata(fieldMetadata))
            {
                return json.Value<bool>();
            }
            if (IsDecfloatMetadata(fieldMetadata))
            {
                // DECFLOAT is returned as string to preserve full precision
                return json.Value<string>();
            }
            if (IsTimestampNtzMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFDataType.TIMESTAMP_NTZ, fieldType, sessionTimezone);
            }
            if (IsTimestampLtzMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFDataType.TIMESTAMP_LTZ, fieldType, sessionTimezone);
            }
            if (IsTimestampTzMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFDataType.TIMESTAMP_TZ, fieldType, sessionTimezone);
            }
            if (IsTimeMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFDataType.TIME, fieldType, sessionTimezone);
            }
            if (IsDateMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                return s_timeConverter.Convert(value, SFDataType.DATE, fieldType, sessionTimezone);
            }
            if (IsBinaryMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(byte[]))
                {
                    return SFDataConverter.HexToBytes(value);
                }
                throw new StructuredTypesReadingException($"Could not read BINARY type into {fieldType}");
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
            throw new StructuredTypesReadingException($"Could not read {fieldMetadata.type} type into {fieldType}");
        }

        private static object ConvertToArray(Type type, Type elementType, List<FieldMetadata> fields, StructurePath structurePath, JToken json, TimeZoneInfo sessionTimezone)
        {
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Array)
            {
                throw new StructuredTypesReadingException($"JSON value is not a JSON array. Occured for path {structurePath}");
            }
            var jsonArray = (JArray)json;
            var arrayType = GetArrayType(type, elementType);
            var result = (IList)Activator.CreateInstance(arrayType, jsonArray.Count);
            var elementMetadata = fields[0];
            for (var i = 0; i < jsonArray.Count; i++)
            {
                var arrayElementPath = structurePath.WithArrayIndex(i);
                result[i] = ConvertToStructuredOrUnstructuredValue(elementType, elementMetadata, arrayElementPath, jsonArray[i], sessionTimezone);
            }
            if (type != arrayType)
            {
                var listType = type.IsAbstract ? typeof(List<>).MakeGenericType(elementType) : type;
                var list = (IList)Activator.CreateInstance(listType);
                for (int i = 0; i < result.Count; i++)
                {
                    list.Add(result[i]);
                }
                return list;
            }

            return result;
        }

        private static object ConvertToMap(Type type, Type keyType, Type valueType, List<FieldMetadata> fields, StructurePath structurePath, JToken json, TimeZoneInfo sessionTimezone)
        {
            if (keyType != typeof(string)
                && keyType != typeof(int) && keyType != typeof(int?)
                && keyType != typeof(long) && keyType != typeof(long?))
            {
                throw new StructuredTypesReadingException($"Unsupported key type of dictionary {keyType} for extracting a map. Occured for path {structurePath}");
            }
            if (json == null || json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Object)
            {
                throw new StructuredTypesReadingException($"Extracting a map failed. JSON value is not a JSON object. Occured for path {structurePath}");
            }
            if (fields == null || fields.Count != 2)
            {
                throw new StructuredTypesReadingException($"Extracting a map failed. Map metadata should have 2 metadata fields. Occured for path {structurePath}");
            }
            var keyMetadata = fields[0];
            var fieldMetadata = fields[1];
            var jsonObject = (JObject)json;
            var dictionaryType = type.IsAbstract ? typeof(Dictionary<,>).MakeGenericType(keyType, valueType) : type;
            var result = (IDictionary)Activator.CreateInstance(dictionaryType);
            using (var jsonEnumerator = jsonObject.GetEnumerator())
            {
                var elementIndex = -1;
                while (jsonEnumerator.MoveNext())
                {
                    elementIndex++;
                    var mapElementPath = structurePath.WithMapIndex(elementIndex);
                    var jsonPropertyWithValue = jsonEnumerator.Current;
                    var fieldValue = jsonPropertyWithValue.Value;
                    var key = IsTextMetadata(keyMetadata) || IsFixedMetadata(keyMetadata)
                        ? ConvertToUnstructuredType(keyMetadata, keyType, jsonPropertyWithValue.Key, sessionTimezone)
                        : throw new StructuredTypesReadingException($"Unsupported key type for map {keyMetadata.type}. Occured for path {mapElementPath}");
                    var value = ConvertToStructuredOrUnstructuredValue(valueType, fieldMetadata, mapElementPath, fieldValue, sessionTimezone);
                    result.Add(key, value);
                }
            }
            return result;
        }

        private static object ConvertToStructuredOrUnstructuredValue(Type valueType, FieldMetadata fieldMetadata, StructurePath structurePath, JToken fieldValue, TimeZoneInfo sessionTimezone)
        {
            try
            {
                if (IsObjectMetadata(fieldMetadata) && IsStructuredMetadata(fieldMetadata))
                {
                    return ConvertToObject(valueType, fieldMetadata.fields, structurePath, fieldValue, sessionTimezone);
                }

                if (IsArrayMetadata(fieldMetadata) && IsStructuredMetadata(fieldMetadata))
                {
                    var nestedType = GetNestedType(valueType);
                    return ConvertToArray(valueType, nestedType, fieldMetadata.fields, structurePath, fieldValue, sessionTimezone);
                }

                if (IsMapMetadata(fieldMetadata) && IsStructuredMetadata(fieldMetadata))
                {
                    var keyValueTypes = GetMapKeyValueTypes(valueType);
                    return ConvertToMap(valueType, keyValueTypes[0], keyValueTypes[1], fieldMetadata.fields, structurePath, fieldValue, sessionTimezone);
                }

                return ConvertToUnstructuredType(fieldMetadata, valueType, fieldValue, sessionTimezone);
            }
            catch (Exception e)
            {
                if (e is SnowflakeDbException)
                    throw;
                throw StructuredTypesReadingHandler.ToSnowflakeDbException(e, $"when reading path {structurePath}");
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
            throw new StructuredTypesReadingException("Only arrays and lists are supported when extracting a structured array");
        }

        private static Type[] GetMapKeyValueTypes(Type type)
        {
            var genericParamWithTwoArguments = type.IsGenericType && type.GenericTypeArguments.Length == 2;
            if (!genericParamWithTwoArguments)
            {
                throw new StructuredTypesReadingException("Could not get key and value types");
            }
            return type.GenericTypeArguments;
        }

        private static Type GetArrayType(Type type, Type elementType)
        {
            if (type.IsArray)
            {
                return type;
            }
            if (IsListType(type) || IsIListType(type))
            {
                return elementType.MakeArrayType();
            }
            throw new StructuredTypesReadingException("Only arrays and lists are supported when extracting a structured array");
        }

        private static bool IsListType(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);

        private static bool IsIListType(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>);

        private static bool IsStructuredMetadata(FieldMetadata fieldMetadata) =>
            fieldMetadata.fields != null && fieldMetadata.fields.Count > 0;

        private static bool IsObjectMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.OBJECT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        internal static bool IsObjectType(string type) =>
            SFDataType.OBJECT.ToString().Equals(type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTextMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TEXT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsFixedMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.FIXED.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampNtzMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_NTZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampLtzMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_LTZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimestampTzMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TIMESTAMP_TZ.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTimeMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.TIME.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsDateMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.DATE.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsRealMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.REAL.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsDecfloatMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.DECFLOAT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsBooleanMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.BOOLEAN.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsBinaryMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.BINARY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsArrayMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.ARRAY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        internal static bool IsArrayType(string type) =>
            SFDataType.ARRAY.ToString().Equals(type, StringComparison.OrdinalIgnoreCase);

        internal static bool IsVectorType(string type) =>
            SFDataType.VECTOR.ToString().Equals(type, StringComparison.OrdinalIgnoreCase);

        private static bool IsVariantMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.VARIANT.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsMapMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.MAP.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        internal static bool IsMapType(string type) =>
            SFDataType.MAP.ToString().Equals(type, StringComparison.OrdinalIgnoreCase);
    }
}
