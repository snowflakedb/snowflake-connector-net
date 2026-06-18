namespace Snowflake.Data.Core
{
    internal class BindingDTO
    {
        public BindingDTO(string type, object value)
        {
            this.type = type;
            this.value = value;
        }

        public string type { get; set; }

        public object value { get; set; }
    }
}
