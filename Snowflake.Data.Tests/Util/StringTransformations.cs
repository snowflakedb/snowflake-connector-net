using System.Collections.Generic;

namespace Snowflake.Data.Tests.Util
{
    internal class StringTransformations
    {
        private List<StringTransformation> _transformations = new();

        public static readonly StringTransformations NoTransformationsInstance = new StringTransformations();

        public string Transform(string value)
        {
            var transformedValue = value;
            foreach (var transformation in _transformations)
            {
                transformedValue = transformation.Transform(transformedValue);
            }
            return transformedValue;
        }

        public StringTransformations ThenTransform(string oldValue, string newValue) =>
            ThenTransform(new StringTransformation(oldValue, newValue));

        private StringTransformations ThenTransform(StringTransformation transformation)
        {
            _transformations.Add(transformation);
            return this;
        }
    }
}
