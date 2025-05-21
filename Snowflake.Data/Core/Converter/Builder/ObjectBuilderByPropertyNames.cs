using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter.Builder
{
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
                throw new StructuredTypesReadingException($"Could not find property: {sfPropertyName}");
            }
            _currentProperty = _type.GetProperty(clientPropertyName);
            if (_currentProperty == null)
            {
                throw new StructuredTypesReadingException($"Could not find property: {sfPropertyName}");
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
}
