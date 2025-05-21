using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core.Converter.Builder
{
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
                throw new StructuredTypesReadingException($"Proper constructor not found for type: {type}");
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
