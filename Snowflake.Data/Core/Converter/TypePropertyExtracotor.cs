using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter
{
    internal static class ObjectBuilderFactory
    {
        public static IObjectBuilder Create(Type type, int fieldsCount, StructureTypeConstructionMethod constructionMethod)
        {
            if (constructionMethod == StructureTypeConstructionMethod.PROPERTIES_NAMES)
            {
                return new ObjectBuilderByPropertyNames(type);
            }
            if (constructionMethod == StructureTypeConstructionMethod.PROPERTIES_ORDER)
            {
                return new ObjectBuilderByPropertyOrder(type);
            }
            if (constructionMethod == StructureTypeConstructionMethod.CONSTRUCTOR)
            {
                return new ObjectBuilderByConstructor(type, fieldsCount);
            }
            throw new Exception("Unknown construction method");
        }
    }

    internal interface IObjectBuilder
    {
        void BuildPart(object value);

        Type MoveNext(string propertyName);

        object Build();
    }

    internal class ObjectBuilderByPropertyNames : IObjectBuilder
    {
        private Type _type;
        private List<Tuple<PropertyInfo, object>> _result;
        private PropertyInfo _currentProperty;

        public ObjectBuilderByPropertyNames(Type type)
        {
            _type = type;
            _result = new List<Tuple<PropertyInfo, object>>();
        }

        public void BuildPart(object value)
        {
            _result.Add(Tuple.Create(_currentProperty, value));
        }

        public Type MoveNext(string propertyName)
        {
            _currentProperty = _type.GetProperty(propertyName);
            if (_currentProperty == null)
            {
                throw new Exception($"Could not find property: {propertyName}");
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
        private Type _type;
        private PropertyInfo[] _properties;
        private int _index;
        private List<Tuple<PropertyInfo, object>> _result;
        private PropertyInfo _currentProperty;

        public ObjectBuilderByPropertyOrder(Type type)
        {
            _type = type;
            _properties = type.GetProperties();
            _index = -1;
            _result = new List<Tuple<PropertyInfo, object>>();
        }

        public void BuildPart(object value)
        {
            _result.Add(Tuple.Create(_currentProperty, value));
        }

        public Type MoveNext(string propertyName)
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

        public Type MoveNext(string propertyName)
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
