using System;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Converter
{
    internal class StructuredTypesReadingHandler
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<StructuredTypesReadingHandler>();

        public static SnowflakeDbException ToSnowflakeDbException(Exception exception, string context)
        {
            if (exception is StructuredTypesReadingException)
            {
                s_logger.Debug("Exception caught when reading structured types", exception);
                return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR, context, exception.Message);
            }
            s_logger.Debug("Exception caught when reading structured types");
            return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_ERROR, context);
        }
    }
}
