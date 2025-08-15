using Apache.Arrow.Types;
using Apache.Arrow;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Reflection;
using System;
using System.Linq;
using System.Collections;

namespace Snowflake.Data.Core.Converter
{
    internal static class ArrowConverter
    {
        internal static T ToObject<T>(Dictionary<string, object> dict) where T : new()
        {
            T obj = new T();
            Type type = typeof(T);

            if (type.GetCustomAttributes(false).Any(attribute => attribute.GetType() == typeof(SnowflakeObject)))
            {
                var constructionMethod = JsonToStructuredTypeConverter.GetConstructionMethod(type);
                if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)
                {
                    foreach (var kvp in dict)
                    {
                        var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                        if (prop != null)
                        {
                            var converted = Convert.ChangeType(kvp.Value, prop.PropertyType);
                            prop.SetValue(obj, converted);
                        }
                        else
                        {
                            var match = type
                            .GetProperties()
                            .SelectMany(
                                property => property.GetCustomAttributes().OfType<SnowflakeColumn>(),
                                (property, attr) => new { Property = property, Attribute = attr }
                            )
                            .FirstOrDefault(x => x.Attribute?.Name == kvp.Key);

                            if (match != null)
                            {
                                var converted = ConvertValue(kvp.Value, match.Property.PropertyType);
                                match.Property.SetValue(obj, converted);
                            }
                        }
                    }
                }
                else if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)
                {
                    var index = 0;
                    foreach (var property in type.GetProperties())
                    {
                        if (index < dict.Count)
                        {
                            var attributes = property.GetCustomAttributes();
                            if (attributes.Count() == 0)
                            {
                                var converted = ConvertValue(dict.ElementAt(index).Value, property.PropertyType);
                                property.SetValue(obj, converted);
                                index++;
                            }
                            else
                            {
                                foreach (var attr in attributes)
                                {
                                    var snowflakeAttr = (SnowflakeColumn)attr;
                                    if (!snowflakeAttr.IgnoreForPropertyOrder)
                                    {
                                        var converted = ConvertValue(dict.ElementAt(index).Value, property.PropertyType);
                                        property.SetValue(obj, converted);
                                        index++;
                                    }
                                }
                            }
                        }
                    }
                }
                else if (constructionMethod == SnowflakeObjectConstructionMethod.CONSTRUCTOR)
                {
                    var constructors = type.GetConstructors();

                    var matchingConstructor = type.GetConstructors()
                        .Where(c => c.GetParameters().Length == dict.Count)
                        .First();

                    if (matchingConstructor == null)
                        throw new StructuredTypesReadingException($"No constructor found for type: {type}");

                    var parameters = new object[dict.Count];
                    var index = 0;
                    foreach (var property in matchingConstructor.GetParameters())
                    {
                        var converted = ConvertValue(dict.ElementAt(index).Value, property.ParameterType);
                        parameters[index] = converted;
                        index++;
                    }

                    return (T)matchingConstructor.Invoke(parameters);
                }
            }
            else
            {
                foreach (var kvp in dict)
                {
                    if (kvp.Value is IList ilist)
                    {
                        foreach (var item in ilist)
                        {
                            Console.WriteLine(item);
                        }
                    }
                    var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                    if (prop != null)
                    {
                        var value = kvp.Value;
                        if (value is List<object> objList)
                        {
                            if (prop.PropertyType.IsArray)
                            {
                                var innerType = prop.PropertyType.GetElementType();
                                var arr = CallMethod(innerType, objList, "ToArray");
                                prop.SetValue(obj, arr);
                            }
                            else if (prop.PropertyType.IsGenericType)
                            {
                                var genericType = prop.PropertyType.GetGenericTypeDefinition();
                                if (genericType == typeof(List<>) || genericType == typeof(IList<>))
                                {
                                    var innerType = prop.PropertyType.GetGenericArguments()[0];
                                    var list = CallMethod(innerType, objList, "ToList");
                                    prop.SetValue(obj, list);
                                }
                            }
                        }
                        else if (value is Dictionary<object, object> objDict)
                        {
                            var genericArgs = prop.PropertyType.GetGenericArguments();
                            var keyType = genericArgs[0];
                            var valueType = genericArgs[1];
                            var dictValue = CallMethod(keyType, objDict, "ToDictionary", valueType);
                            prop.SetValue(obj, dictValue);
                        }
                        else if (value is Dictionary<string, object> nestedDict)
                        {
                            var nestedObj = typeof(ArrowConverter)
                                .GetMethod("ToObject", BindingFlags.NonPublic | BindingFlags.Static)
                                .MakeGenericMethod(prop.PropertyType)
                                .Invoke(null, new object[] { nestedDict });
                            prop.SetValue(obj, nestedObj);
                        }
                        else
                        {
                            var converted = Convert.ChangeType(value, prop.PropertyType);
                            prop.SetValue(obj, converted);
                        }
                    }
                }
            }
            return obj;
        }

        internal static object CallMethod(Type type, object obj, string methodName, Type type2 = null)
        {
            MethodInfo genericMethod = typeof(ArrowConverter)
                .GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
            MethodInfo constructedMethod = type2 == null
                ? genericMethod.MakeGenericMethod(type)
                : genericMethod.MakeGenericMethod(type, type2);
            return constructedMethod.Invoke(null, new object[] { obj });
        }

        internal static T[] ToArray<T>(List<object> list)
        {
            var targetType = typeof(T);
            var result = new T[list.Count];
            for (int i = 0; i < list.Count; i++)
            {
                if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    targetType = Nullable.GetUnderlyingType(targetType);
                result[i] = (T)ConvertValue(list[i], targetType);
            }
            return result;
        }

        internal static List<T> ToList<T>(List<object> list)
        {
            var targetType = typeof(T);
            var result = new List<T>(list.Count);
            foreach (var item in list)
            {
                result.Add((T)ConvertValue(item, targetType));
            }
            return result;
        }

        internal static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(Dictionary<object, object> dict)
        {
            var keyType = typeof(TKey);
            var valueType = typeof(TValue);

            var result = new Dictionary<TKey, TValue>();
            foreach (var kvp in dict)
            {
                var key = (TKey)ConvertValue(kvp.Key, keyType);
                var value = (TValue)ConvertValue(kvp.Value, valueType);
                result[key] = value;
            }
            return result;
        }

        private static object ConvertValue(object value, Type targetType)
        {
            if (value == null)
                return null;

            if (targetType.IsAssignableFrom(value.GetType()))
                return value;

            if (value is Dictionary<string, object> dict)
                return CallMethod(targetType, dict, "ToObject");

            if (value is Dictionary<object, object> objDict && targetType.IsGenericType &&
                targetType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                var keyType = targetType.GetGenericArguments()[0];
                var valueType = targetType.GetGenericArguments()[1];
                return CallMethod(keyType, objDict, "ToDictionary", valueType);
            }

            if (value is List<object> objList)
            {
                if (targetType.IsArray)
                {
                    var elementType = targetType.GetElementType();
                    return CallMethod(elementType, objList, "ToArray");
                }
                else if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    var elementType = targetType.GetGenericArguments()[0];
                    return CallMethod(elementType, objList, "ToList");
                }
            }
            return Convert.ChangeType(value, targetType);
        }

        internal static object ConvertArrowValue(IArrowArray array, int index)
        {
            switch (array)
            {
                case StructArray strct: return ParseStructArray(strct, index);
                case MapArray map: return ParseMapArray(map, index);
                case ListArray list: return ParseListArray(list, index);
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

        internal static Dictionary<string, object> ParseStructArray(StructArray structArray, int index)
        {
            var result = new Dictionary<string, object>();
            var structTypeFields = ((StructType)structArray.Data.DataType).Fields;

            for (int i = 0; i < structArray.Fields.Count; i++)
            {
                var field = structArray.Fields[i];
                var fieldName = structTypeFields[i].Name;
                var value = ConvertArrowValue(field, index);

                if (value == null && structArray.Fields.Count == 1)
                    return null;

                result[fieldName] = value;
            }

            return result;
        }

        internal static List<object> ParseListArray(ListArray listArray, int index)
        {
            int start = listArray.ValueOffsets[index];
            int end = listArray.ValueOffsets[index + 1];

            if (start == end)
                return null;

            var values = listArray.Values;
            var result = new List<object>(end - start);

            for (int i = start; i < end; i++)
            {
                result.Add(ConvertArrowValue(values, i));
            }

            return result;
        }

        internal static Dictionary<object, object> ParseMapArray(MapArray mapArray, int index)
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
                var key = ConvertArrowValue(keyArray, i);
                var value = ConvertArrowValue(valueArray, i);
                result[key] = value;
            }

            return result;
        }
    }
}
