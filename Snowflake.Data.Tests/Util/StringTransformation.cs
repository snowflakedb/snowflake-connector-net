namespace Snowflake.Data.Tests.Util
{
    internal class StringTransformation
    {
        private readonly string _oldValue;
        private readonly string _newValue;

        public static readonly StringTransformation NoTransformationInstance = new(null, null);

        public StringTransformation(string oldValue, string newValue)
        {
            _oldValue = oldValue;
            _newValue = newValue;
        }

        public string Transform(string value)
        {
            if (string.IsNullOrEmpty(_oldValue) || string.IsNullOrEmpty(value))
                return value;
            return value.Replace(_oldValue, _newValue);
        }
    }
}
