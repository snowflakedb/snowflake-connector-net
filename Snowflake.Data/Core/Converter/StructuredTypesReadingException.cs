using System;

namespace Snowflake.Data.Core.Converter
{
    internal class StructuredTypesReadingException : Exception
    {
        public StructuredTypesReadingException(string message) : base(message)
        {
        }
    }
}
