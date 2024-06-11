using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter
{
    internal class JsonToStructuredTypeConverter
    {
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
                    else
                    {
                        var unstructuredValue = ConvertToUnstructuredType(fieldMetadata, fieldValue);
                        objectBuilder.BuildPart(unstructuredValue);
                    }
                }
            }
            return objectBuilder.Build();
        }

        private static object ConvertToUnstructuredType(FieldMetadata fieldMetadata, JToken json)
        {
            if (IsTextMetadata(fieldMetadata))
            {
                return json.Value<string>();
            }
            else
            {
                throw new Exception("Case not implemented yet");
            }
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
            var result = (object[]) Activator.CreateInstance(arrayType, jsonArray.Count);
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
                else
                {
                    result[i] = ConvertToUnstructuredType(elementMetadata, jsonArray[i]);
                }
            }
            if (type != arrayType)
            {
                var list = (IList) Activator.CreateInstance(type);
                for (int i = 0; i < result.Length; i++)
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
                        var unstructuredValue = ConvertToUnstructuredType(fieldMetadata, fieldValue);
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

        private static bool IsArrayMetadata(FieldMetadata fieldMetadata) =>
            SFDataType.ARRAY.ToString().Equals(fieldMetadata.type, StringComparison.OrdinalIgnoreCase);
    }
}
