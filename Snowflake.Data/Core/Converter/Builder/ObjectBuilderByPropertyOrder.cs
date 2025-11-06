using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter.Builder
{
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
            _properties = GetProperties(type);
            _index = -1;
            _result = new List<Tuple<PropertyInfo, object>>();
        }

        internal static PropertyInfo[] GetProperties(Type type)
        {
            return type.GetProperties().Where(property => !IsIgnoredForPropertiesOrder(property)).ToArray();
        }

        private static bool IsIgnoredForPropertiesOrder(PropertyInfo property)
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
}
