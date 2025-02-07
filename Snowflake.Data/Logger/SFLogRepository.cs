/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;

internal static class SFLogRepository
{
    internal static SFLogger s_rootLogger = new SFLoggerImpl(typeof(SFLogRepository));

    internal static SFLogger GetRootLogger()
    {
        return s_rootLogger;
    }
}
