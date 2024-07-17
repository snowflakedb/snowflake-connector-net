using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter
{
    internal class JsonToStructuredTypeConverter
    {

        private static TimeConverter _timeConverter = new TimeConverter();

        public static T Convert<T>(string sourceTypeName, List<FieldMetadata> fields, JObject value,
            StructureTypeConstructionMethod constructionMethod)
        {
            var type = typeof(T);
            if (SFDataType.OBJECT.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (T) ConvertToObject(type, fields, value, constructionMethod);
            }

            throw new Exception("Case not supported");
        }

        public static T[] ConvertArray<T>(string sourceTypeName, List<FieldMetadata> fields, JArray value,
            StructureTypeConstructionMethod constructionMethod)
        {
            var type = typeof(T[]);
            var elementType = typeof(T);
            if (SFDataType.ARRAY.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (T[]) ConvertToArray(type, elementType, fields, value, constructionMethod);
            }

            throw new Exception("Case not supported");
        }

        public static Dictionary<TKey, TValue> ConvertMap<TKey, TValue>(string sourceTypeName, List<FieldMetadata> fields, JObject value,
            StructureTypeConstructionMethod constructionMethod)
        {
            var type = typeof(Dictionary<TKey, TValue>);
            var keyType = typeof(TKey);
            var valueType = typeof(TValue);
            if (SFDataType.MAP.ToString().Equals(sourceTypeName, StringComparison.OrdinalIgnoreCase))
            {
                return (Dictionary<TKey, TValue>) ConvertToMap(type, keyType, valueType, fields, value, constructionMethod);
            }

            throw new Exception("Case not supported");
        }


        private static object ConvertToObject(Type type, List<FieldMetadata> fields, JToken json, StructureTypeConstructionMethod constructionMethod)
        {
            if (json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Object)
            {
                throw new Exception("Json is not an object");
            }
            var jsonObject = (JObject)json;
            var objectBuilder = ObjectBuilderFactory.Create(type, fields?.Count ?? 0, constructionMethod);
            using (var jsonEnumerator = jsonObject.GetEnumerator())
            {

                var metadataIterator = fields.GetEnumerator();
                while (jsonEnumerator.MoveNext() && metadataIterator.MoveNext())
                {
                    var jsonPropertyWithValue = jsonEnumerator.Current;
                    var fieldMetadata = metadataIterator.Current;
                    var key = jsonPropertyWithValue.Key;
                    var fieldValue = jsonPropertyWithValue.Value;
                    var fieldType = objectBuilder.MoveNext(key);
                    if (IsObjectMetadata(fieldMetadata))
                    {
                        var objectValue = ConvertToObject(fieldType, fieldMetadata.fields, fieldValue, constructionMethod);
                        objectBuilder.BuildPart(objectValue);
                    }
                    else if (IsArrayMetadata(fieldMetadata))
                    {
                        var nestedType = GetNestedType(fieldType);
                        var arrayValue = ConvertToArray(fieldType, nestedType, fieldMetadata.fields, fieldValue, constructionMethod);
                        objectBuilder.BuildPart(arrayValue);
                    }
                    else if (IsMapMetadata(fieldMetadata))
                    {
                        var keyValueTypes = GetMapKeyValueTypes(fieldType);
                        var mapValue = ConvertToMap(fieldType, keyValueTypes[0], keyValueTypes[1], fieldMetadata.fields, fieldValue, constructionMethod);
                        objectBuilder.BuildPart(mapValue);
                    }
                    else
                    {
                        var unstructuredValue = ConvertToUnstructuredType(fieldMetadata, fieldType, fieldValue);
                        objectBuilder.BuildPart(unstructuredValue);
                    }
                }
            }
            return objectBuilder.Build();
        }

        private static object ConvertToUnstructuredType(FieldMetadata fieldMetadata, Type fieldType, JToken json)
        {
            // var value = json.Value<string>();
            // var bytes = Encoding.UTF8.GetBytes(value);
            // var sfType = Enum.Parse<SFDataType>(fieldMetadata.type, true);
            // var cSharpVal = SFDataConverter.ConvertToCSharpVal(new UTF8Buffer(bytes), sfType, fieldType);
            // return cSharpVal == DBNull.Value ? null : cSharpVal;

            if (IsTextMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(char))
                {
                    return char.Parse(value);
                }
                if (fieldType == typeof(Guid))
                {
                    return Guid.Parse(value);
                }
                return value;
            }
            if (IsFixedMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                if (fieldType == typeof(byte))
                {
                    return byte.Parse(value);
                }
                if (fieldType == typeof(sbyte))
                {
                    return sbyte.Parse(value);
                }
                if (fieldType == typeof(short))
                {
                    return short.Parse(value);
                }
                if (fieldType == typeof(ushort))
                {
                    return ushort.Parse(value);
                }
                if (fieldType == typeof(Int32))
                {
                    var bytes = Encoding.UTF8.GetBytes(value);
                    return FastParser.FastParseInt32(bytes, 0, bytes.Length);
                }
                if (fieldType == typeof(Int64))
                {
                    var bytes = Encoding.UTF8.GetBytes(value);
                    return FastParser.FastParseInt64(bytes, 0, bytes.Length);
                }
                throw new Exception("Case not implemented yet");
            }
            if (IsRealMetadata(fieldMetadata))
            {
                var value = json.Value<string>();
                var bytes = Encoding.UTF8.GetBytes(value);
                var decimalValue = FastParser.FastParseDecimal(bytes, 0, bytes.Length);
                if (fieldType == typeof(float))
                {
                    return (float) decimalValue;
                }
                if (fieldType == typeof(double))
                {
                    return (double) decimalValue;
                }
                return decimalValue;
            }
            if (IsBooleanMetadata(fieldMetadata))
            {
                var value = json.Value<bool>();
                return value;
            }
            if (IsTimestampNtz(fieldMetadata))
            {
                var value = json.Value<string>();
                return _timeConverter.Convert(value, SFTimestampType.TIMESTAMP_NTZ, fieldType);
            }
            if (IsTimestampLtz(fieldMetadata))
            {
                var value = json.Value<string>();
                return _timeConverter.Convert(value, SFTimestampType.TIMESTAMP_LTZ, fieldType);
            }
            if (IsTimestampTz(fieldMetadata))
            {
                var value = json.Value<string>();
                return _timeConverter.Convert(value, SFTimestampType.TIMESTAMP_TZ, fieldType);
            }
            if (IsTime(fieldMetadata))
            {
                var value = json.Value<string>();
                return _timeConverter.Convert(value, SFTimestampType.TIME, fieldType);
            }
            if (IsDate(fieldMetadata))
            {
                var value = json.Value<string>();
                return _timeConverter.Convert(value, SFTimestampType.DATE, fieldType);
            }
            throw new Exception("Case not implemented yet");
        }

        private static object ConvertToArray(Type type, Type elementType, List<FieldMetadata> fields, JToken json, StructureTypeConstructionMethod constructionMethod)
        {
            if (json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
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
                if (IsObjectMetadata(elementMetadata))
                {
                    result[i] = ConvertToObject(elementType, elementMetadata.fields, jsonArray[i], constructionMethod);
                }
                else if (IsArrayMetadata(elementMetadata))
                {
                    var nestedType = elementType.GetElementType();
                    result[i] = ConvertToArray(elementType, nestedType, elementMetadata.fields, jsonArray[i], constructionMethod);
                }
                else if (IsMapMetadata(elementMetadata))
                {
                    var keyValueTypes = GetMapKeyValueTypes(elementType);
                    result[i] = ConvertToMap(elementType, keyValueTypes[0], keyValueTypes[1], elementMetadata.fields, jsonArray[i], constructionMethod);
                }
                else
                {
                    result[i] = ConvertToUnstructuredType(elementMetadata, elementType, jsonArray[i]);
                }
            }
            if (type != arrayType)
            {
                var list = (IList) Activator.CreateInstance(type);
                for (int i = 0; i < result.Count; i++)
                {
                    list.Add(result[i]);
                }
                return list;
            }

            return result;
        }

        private static object ConvertToMap(Type type, Type keyType, Type valueType, List<FieldMetadata> fields, JToken json,
            StructureTypeConstructionMethod constructionMethod)
        {
            if (keyType != typeof(string))
            {
                throw new Exception("Usuported key type in dictionary");
            }
            if (json.Type == JTokenType.Null || json.Type == JTokenType.Undefined)
            {
                return null;
            }
            if (json.Type != JTokenType.Object)
            {
                throw new Exception("Json is not an object");
            }
            if (fields == null || fields.Count != 2)
            {
                throw new Exception("Expecting map to have 2 metadata fields");
            }
            var keyMetadata = fields[0];
            var fieldMetadata = fields[1];
            var jsonObject = (JObject)json;
            var result = (IDictionary) Activator.CreateInstance(type);
            using (var jsonEnumerator = jsonObject.GetEnumerator())
            {
                while (jsonEnumerator.MoveNext())
                {
                    var jsonPropertyWithValue = jsonEnumerator.Current;
                    var fieldValue = jsonPropertyWithValue.Value;
                    var key = IsTextMetadata(keyMetadata) ? jsonPropertyWithValue.Key : throw new Exception("Unsupported type of map key");
                    if (IsObjectMetadata(fieldMetadata))
                    {
                        var objectValue = ConvertToObject(valueType, fieldMetadata.fields, fieldValue, constructionMethod);
                        result.Add(key, objectValue);
                    }
                    else if (IsArrayMetadata(fieldMetadata))
                    {
                        var nestedType = GetNestedType(valueType);
                        var arrayValue = ConvertToArray(valueType, nestedType, fieldMetadata.fields, fieldValue, constructionMethod);
                        result.Add(key, arrayValue);
                    }
                    else
                    {
                        var unstructuredValue = ConvertToUnstructuredType(fieldMetadata, valueType, fieldValue);
                        result.Add(key, unstructuredValue);
                    }
                }
            }
            return result;
        }

        private static Type GetNestedType(Type type)
        {
            if (type.IsArray)
            {
                return type.GetElementType();
            }
            if (IsListType(type))
            {
                return type.GenericTypeArguments[0];
            }
            throw new Exception("neither array nor list");
        }

        private static Type[] GetMapKeyValueTypes(Type type)
        {
            // TODO: type.GetGenericTypeDefinition() - make sure it is a map
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
            if (IsListType(type))
            {
                return elementType.MakeArrayType();
            }
            throw new Exception("Neither array nor list");
        }

        // JValue, JObject, JArray ... are elements of JArray

        private static bool IsListType(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);

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

        private static bool IsArrayMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.ARRAY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);

        private static bool IsMapMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.MAP.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);
    }
}
