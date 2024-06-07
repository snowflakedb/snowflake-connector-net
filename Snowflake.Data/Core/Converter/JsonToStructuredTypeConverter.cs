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
                    if (IsTextMetadata(fieldMetadata))
                    {
                        var stringValue = fieldValue.Value<string>();
                        var fieldType = objectBuilder.MoveNext(key);
                        objectBuilder.BuildPart(stringValue);
                    } else if (IsObjectMetadata(fieldMetadata))
                    {
                        var fieldType = objectBuilder.MoveNext(key);
                        var objectValue = ConvertToObject(fieldType, fieldMetadata.fields, fieldValue, constructionMethod);
                        objectBuilder.BuildPart(objectValue);
                    }
                    else if (IsArrayMetadata(fieldMetadata))
                    {
                        var fieldType = objectBuilder.MoveNext(key);
                        var nestedType = GetNestedType(fieldType);
                        var arrayValue = ConvertToArray(fieldType, nestedType, fieldMetadata.fields, fieldValue, constructionMethod);
                        objectBuilder.BuildPart(arrayValue);
                    }
                    else
                    {
                        throw new Exception("Case not implemented yet");
                    }
                }
            }
            return objectBuilder.Build();
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
                else if (IsTextMetadata(elementMetadata))
                {
                    result[i] = jsonArray[i].Value<string>();
                }
                else if (IsArrayMetadata(elementMetadata))
                {
                    var nestedType = elementType.GetElementType();
                    result[i] = ConvertToArray(elementType, nestedType, elementMetadata.fields, jsonArray[i], constructionMethod);
                }
                else
                {
                    throw new Exception("Case not implemented yet");
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
