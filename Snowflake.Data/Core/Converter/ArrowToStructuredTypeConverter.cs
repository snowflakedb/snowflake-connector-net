using Apache.Arrow.Types;
using Apache.Arrow;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Reflection;
using System;
using System.Linq;

namespace Snowflake.Data.Core.Converter
{
    internal static class ArrowConverter
    {
        internal static T ToObject<T>(Dictionary<string, object> dict) where T : new()
        {
            T obj = new T();
            Type type = typeof(T);
            var constructionMethod = JsonToStructuredTypeConverter.GetConstructionMethod(type);

            if (type.GetCustomAttributes(false).Any(attr => attr.GetType() == typeof(SnowflakeObject)))
            {
                if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)
                    SetPropertiesFromDictionary(obj, type, dict);
                else if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)
                    SetPropertiesFromOrder(obj, type, dict);
                else if (constructionMethod == SnowflakeObjectConstructionMethod.CONSTRUCTOR)
                    return CreateFromConstructor<T>(type, dict);
            }
            else
                SetPropertiesFromDictionary(obj, type, dict);

            return obj;
        }

        private static void SetPropertiesFromDictionary<T>(T obj, Type type, Dictionary<string, object> dict)
        {
            foreach (var kvp in dict)
            {
                var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                if (prop != null)
                {
                    var value = ConvertValue(kvp.Value, prop.PropertyType);
                    prop.SetValue(obj, value);
                }
                else
                {
                    HandleNestedDictionary<T>(obj, kvp, type);
                }
            }
        }

        private static void SetPropertiesFromOrder<T>(T obj, Type type, Dictionary<string, object> dict)
        {
            var index = 0;
            foreach (var property in type.GetProperties())
            {
                if (index >= dict.Count) break;
                var converted = ConvertValue(dict.ElementAt(index).Value, property.PropertyType);
                property.SetValue(obj, converted);
                index++;
            }
        }

        private static T CreateFromConstructor<T>(Type type, Dictionary<string, object> dict)
        {
            var matchingConstructor = type.GetConstructors()
                .FirstOrDefault(c => c.GetParameters().Length == dict.Count)
                ?? throw new StructuredTypesReadingException($"No constructor found for type: {type}");

            var parameters = dict.Values.Select((v, i) => ConvertValue(v, matchingConstructor.GetParameters()[i].ParameterType)).ToArray();
            return (T)matchingConstructor.Invoke(parameters);
        }

        private static void HandleNestedDictionary<T>(T obj, KeyValuePair<string, object> kvp, Type type)
        {
            if (kvp.Value is Dictionary<string, object> nestedDict)
            {
                var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                var nestedObj = ToObject<object>(nestedDict);
                prop.SetValue(obj, nestedObj);
            }
        }

        internal static object CallMethod(Type type, object obj, string methodName, Type type2 = null)
        {
            var genericMethod = typeof(ArrowConverter).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
            var constructedMethod = type2 == null
                ? genericMethod.MakeGenericMethod(type)
                : genericMethod.MakeGenericMethod(type, type2);
            return constructedMethod.Invoke(null, new object[] { obj });
        }

        internal static T[] ToArray<T>(List<object> list)
        {
            return list.Select(item => (T)ConvertValue(item, typeof(T))).ToArray();
        }

        internal static List<T> ToList<T>(List<object> list)
        {
            return list.Select(item => (T)ConvertValue(item, typeof(T))).ToList();
        }

        internal static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(Dictionary<object, object> dict)
        {
            return dict.ToDictionary(
                kvp => (TKey)ConvertValue(kvp.Key, typeof(TKey)),
                kvp => (TValue)ConvertValue(kvp.Value, typeof(TValue))
            );
        }

        private static object ConvertValue(object value, Type targetType)
        {
            if (value == null) return null;

            if (targetType.IsAssignableFrom(value.GetType())) return value;

            if (value is Dictionary<string, object> dict)
                return ToObject<object>(dict);
            if (value is Dictionary<object, object> objDict &&
                targetType.IsGenericType &&
                targetType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                return ToDictionary<object, object>(objDict);

            if (value is List<object> objList)
            {
                if (targetType.IsArray) return ToArray<object>(objList);
                if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(List<>))
                    return ToList<object>(objList);
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
