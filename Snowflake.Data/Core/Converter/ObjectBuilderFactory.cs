using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter
{
    internal static class ObjectBuilderFactory
    {
        public static IObjectBuilder Create(Type type, int fieldsCount, SnowflakeObjectConstructionMethod constructionMethod)
        {
            if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)
            {
                return new ObjectBuilderByPropertyNames(type);
            }
            if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)
            {
                return new ObjectBuilderByPropertyOrder(type);
            }
            if (constructionMethod == SnowflakeObjectConstructionMethod.CONSTRUCTOR)
            {
                return new ObjectBuilderByConstructor(type, fieldsCount);
            }
            throw new Exception("Unknown construction method");
        }
    }

    internal interface IObjectBuilder
    {
        void BuildPart(object value);

        Type MoveNext(string sfPropertyName);

        object Build();
    }

    internal class ObjectBuilderByPropertyNames : IObjectBuilder
    {
        private readonly Type _type;
        private readonly List<Tuple<PropertyInfo, object>> _result;
        private PropertyInfo _currentProperty;
        private readonly Dictionary<string, string> _sfToClientPropertyNames;

        public ObjectBuilderByPropertyNames(Type type)
        {
            _type = type;
            _result = new List<Tuple<PropertyInfo, object>>();
            _sfToClientPropertyNames = new Dictionary<string, string>();
            foreach (var property in type.GetProperties())
            {
                var sfPropertyName = GetSnowflakeName(property);
                _sfToClientPropertyNames.Add(sfPropertyName, property.Name);
            }
        }

        private string GetSnowflakeName(PropertyInfo propertyInfo)
        {
            var sfAnnotation = propertyInfo.GetCustomAttributes<SnowflakeColumn>().FirstOrDefault();
            return string.IsNullOrEmpty(sfAnnotation?.Name) ? propertyInfo.Name : sfAnnotation.Name;
        }

        public void BuildPart(object value)
        {
            _result.Add(Tuple.Create(_currentProperty, value));
        }

        public Type MoveNext(string sfPropertyName)
        {
            if (!_sfToClientPropertyNames.TryGetValue(sfPropertyName, out var clientPropertyName))
            {
                throw new Exception($"Could not find property: {sfPropertyName}");
            }
            _currentProperty = _type.GetProperty(clientPropertyName);
            if (_currentProperty == null)
            {
                throw new Exception($"Could not find property: {sfPropertyName}");
            }
            return _currentProperty.PropertyType;
        }

        public object Build()
        {
            var result = Activator.CreateInstance(_type);
            _result.ForEach(p => p.Item1.SetValue(result, p.Item2, null));
            return result;
        }
    }

    internal class ObjectBuilderByPropertyOrder : IObjectBuilder
    {
        private readonly Type _type;
        private readonly PropertyInfo[] _properties;
        private int _index;
        private readonly List<Tuple<PropertyInfo, object>> _result;
        private PropertyInfo _currentProperty;

        public ObjectBuilderByPropertyOrder(Type type)
        {
            _type = type;
            _properties = type.GetProperties().Where(property => !IsIgnoredForPropertiesOrder(property)).ToArray();
            _index = -1;
            _result = new List<Tuple<PropertyInfo, object>>();
        }

        private bool IsIgnoredForPropertiesOrder(PropertyInfo property)
        {
            var sfAnnotation = property.GetCustomAttributes<SnowflakeColumn>().FirstOrDefault();
            return sfAnnotation != null && sfAnnotation.IgnoreForPropertyOrder;
        }

        public void BuildPart(object value)
        {
            _result.Add(Tuple.Create(_currentProperty, value));
        }

        public Type MoveNext(string sfPropertyName)
        {
            _index++;
            _currentProperty = _properties[_index];
            return _currentProperty.PropertyType;
        }

        public object Build()
        {
            var result = Activator.CreateInstance(_type);
            _result.ForEach(p => p.Item1.SetValue(result, p.Item2, null));
            return result;
        }
    }

    internal class ObjectBuilderByConstructor : IObjectBuilder
    {
        private Type _type;
        private List<object> _result;
        private Type[] _parameters;
        private int _index;

        public ObjectBuilderByConstructor(Type type, int fieldsCount)
        {
            _type = type;
            var matchingConstructors = type.GetConstructors()
                .Where(c => c.GetParameters().Length == fieldsCount)
                .ToList();
            if (matchingConstructors.Count == 0)
            {
                throw new Exception($"Proper constructor not found for type: {type}");
            }
            var constructor = matchingConstructors.First();
            _parameters = constructor.GetParameters().Select(p => p.ParameterType).ToArray();
            _index = -1;
            _result = new List<object>();
        }

        public Type MoveNext(string sfPropertyName)
        {
            _index++;
            return _parameters[_index];
        }

        public void BuildPart(object value)
        {
            _result.Add(value);
        }

        public object Build()
        {
            object[] parameters = _result.ToArray();
            return Activator.CreateInstance(_type, parameters);
        }

    }
}
