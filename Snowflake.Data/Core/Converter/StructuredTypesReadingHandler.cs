using System;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Converter
{
    public class StructuredTypesReadingHandler
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<StructuredTypesReadingHandler>();

        public static SnowflakeDbException ToSnowflakeDbException(Exception exception, string context)
        {
            s_logger.Debug("Exception caught when reading structured types", exception);
            if (exception is StructuredTypesReadingException)
            {
                return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR, context, exception.Message);
            }
            return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_ERROR, context);
        }
    }
}
