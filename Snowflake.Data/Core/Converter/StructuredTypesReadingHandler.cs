using System;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Core.Converter
{
    public class StructuredTypesReadingHandler
    {
        private static readonly ILogger s_logger = SFLoggerFactory.GetCustomLogger<StructuredTypesReadingHandler>();

        public static SnowflakeDbException ToSnowflakeDbException(Exception exception, string context)
        {
            if (exception is StructuredTypesReadingException)
            {
                s_logger.LogDebug("Exception caught when reading structured types", exception);
                return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_DETAILED_ERROR, context, exception.Message);
            }
            s_logger.LogDebug("Exception caught when reading structured types");
            return new SnowflakeDbException(SFError.STRUCTURED_TYPE_READ_ERROR, context);
        }
    }
}
