/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;

namespace Snowflake.Data.Log
{
    internal interface SFLogger
    {
        bool IsDebugEnabled();

        bool IsInfoEnabled();

        bool IsWarnEnabled();

        bool IsErrorEnabled();

        void Debug(string msg, Exception ex = null);

        void Info(string msg, Exception ex = null);

        void Warn(string msg, Exception ex = null);

        void Error(string msg, Exception ex = null);

        List<SFAppender> GetAppenders();

        void AddAppender(SFAppender appender);

        void RemoveAppender(SFAppender appender);

        void SetLevel(LoggingEvent level);
    }

    public enum LoggingEvent
    {
        OFF, TRACE, DEBUG, INFO, WARN, ERROR
    }
}
