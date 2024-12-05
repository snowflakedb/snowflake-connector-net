/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;

namespace Snowflake.Data.Log
{
    interface SFLogger
    {
        bool IsDebugEnabled();

        bool IsInformationEnabled();

        bool IsWarningEnabled();

        bool IsErrorEnabled();

        bool IsFatalEnabled();

        void Debug(string msg, Exception ex = null);

        void Information(string msg, Exception ex = null);

        void Warning(string msg, Exception ex = null);

        void Error(string msg, Exception ex = null);

        void Fatal(string msg, Exception ex = null);

        List<SFAppender> GetAppenders();

        void AddAppender(SFAppender appender);

        void RemoveAppender(SFAppender appender);

        void SetLevel(LoggingEvent level);
    }

    public enum LoggingEvent
    {
        OFF, TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    }
}
