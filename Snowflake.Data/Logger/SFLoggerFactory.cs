/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using log4net;

namespace Snowflake.Data.Log
{
    class SFLoggerFactory
    {
        public static SFLogger GetLogger<T>()
        {
            ILog logger = LogManager.GetLogger(typeof(T));
            return new Log4netImpl(logger);
        }
    }
}
