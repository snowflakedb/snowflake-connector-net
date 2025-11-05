using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Reflection;
using System;
using System.Linq;

namespace Snowflake.Data.Core.Converter
{
    internal static class ArrowConverter
    {
        internal static T ConvertObject<T>(Dictionary<string, object> dict) where T : new()
        {
            T obj = new T();
            Type type = typeof(T);
            if (type.GetCustomAttributes(false).Any(attribute => attribute.GetType() == typeof(SnowflakeObject)))
            {
                var constructionMethod = JsonToStructuredTypeConverter.GetConstructionMethod(type);
                switch (constructionMethod)
                {
                    case SnowflakeObjectConstructionMethod.PROPERTIES_NAMES:
                        MapPropertiesByNames(obj, dict, type);
                        break;
                    case SnowflakeObjectConstructionMethod.PROPERTIES_ORDER:
                        MapPropertiesByOrder(obj, dict, type);
                        break;
                    case SnowflakeObjectConstructionMethod.CONSTRUCTOR:
                        return MapUsingConstructor<T>(dict, type);
                }
            }
            else
            {
                foreach (var kvp in dict)
                {
                    var prop = type.GetProperty(kvp.Key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                    if (prop == null)
                        continue;
                    prop.SetValue(obj, ConvertValue(kvp.Value, prop.PropertyType));
                }
            }
            return obj;
        }

        private static void MapPropertiesByNames(object obj, Dictionary<string, object> dict, Type type)
        {
            foreach (var kvp in dict)
            {
                var prop = FindPropertyByName(type, kvp.Key);
                if (prop != null)
                {
                    var converted = ConvertValue(kvp.Value, prop.PropertyType);
                    prop.SetValue(obj, converted);
                }
            }
        }

        private static PropertyInfo FindPropertyByName(Type type, string name)
        {
            var prop = type.GetProperty(name, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
            if (prop != null)
                return prop;

            return type.GetProperties()
                .FirstOrDefault(p => p.GetCustomAttributes()
                    .OfType<SnowflakeColumn>()
                    .Any(attr => attr?.Name == name));
        }

        private static void MapPropertiesByOrder(object obj, Dictionary<string, object> dict, Type type)
        {
            var index = 0;
            foreach (var property in type.GetProperties())
            {
                if (index >= dict.Count)
                    break;

                var attributes = property.GetCustomAttributes().OfType<SnowflakeColumn>().ToList();
                if (attributes.Count == 0 || attributes.All(attr => !attr.IgnoreForPropertyOrder))
                {
                    var converted = ConvertValue(dict.ElementAt(index).Value, property.PropertyType);
                    property.SetValue(obj, converted);
                    index++;
                }
            }
        }

        private static T MapUsingConstructor<T>(Dictionary<string, object> dict, Type type)
        {
            var matchingConstructor = type.GetConstructors()
                .FirstOrDefault(c => c.GetParameters().Length == dict.Count) ??
                throw new StructuredTypesReadingException($"No constructor found for type: {type}");
            var parameters = matchingConstructor.GetParameters()
                .Select((param, index) => ConvertValue(dict.ElementAt(index).Value, param.ParameterType))
                .ToArray();
            return (T)matchingConstructor.Invoke(parameters);
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

        internal static T[] ConvertArray<T>(List<object> list)
        {
            var targetType = typeof(T);
            var result = new T[list.Count];
            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
                targetType = Nullable.GetUnderlyingType(targetType);
            for (int i = 0; i < list.Count; i++)
            {
                result[i] = (T)ConvertValue(list[i], targetType);
            }
            return result;
        }

        private static List<T> ConvertList<T>(List<object> list)
        {
            var targetType = typeof(T);
            var result = new List<T>(list.Count);
            foreach (var item in list)
            {
                result.Add((T)ConvertValue(item, targetType));
            }
            return result;
        }

        internal static Dictionary<TKey, TValue> ConvertMap<TKey, TValue>(Dictionary<object, object> dict)
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
            switch (value)
            {
                case null:
                    return null;
                case var _ when targetType.IsAssignableFrom(value.GetType()):
                    return value;
                case Dictionary<string, object> objDict:
                    return CallMethod(targetType, objDict, nameof(ConvertObject));
                case Dictionary<object, object> mapDict:
                    if (targetType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                    {
                        var genericArgs = targetType.GetGenericArguments();
                        if (genericArgs.Length == 2)
                        {
                            var keyType = genericArgs[0];
                            var valueType = genericArgs[1];
                            return CallMethod(keyType, mapDict, nameof(ConvertMap), valueType);
                        }
                    }
                    goto default;
                case List<object> objList:
                    if (targetType.IsArray)
                    {
                        var elementType = targetType.GetElementType();
                        return CallMethod(elementType, objList, nameof(ConvertArray));
                    }
                    else if (targetType.IsGenericType)
                    {
                        var elementType = targetType.GetGenericArguments()[0];
                        return CallMethod(elementType, objList, nameof(ConvertList));
                    }
                    goto default;
                default:
                    return Convert.ChangeType(value, targetType);
            }
        }
    }
}
