/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Log
{
    interface SFLogger
    {
        bool IsDebugEnabled();

        bool IsInfoEnabled();

        bool IsWarnEnabled();

        bool IsErrorEnabled();

        bool IsFatalEnabled();

        void Debug(string msg, Exception ex=null);

        void DebugFmt(string fmt, params object[] args);

        void Info(string msg, Exception ex=null);

        void InfoFmt(string fmt, params object[] args);

        void Warn(string msg, Exception ex = null);

        void WarnFmt(string fmt, params object[] args);

        void Error(string msg, Exception ex = null);

        void ErrorFmt(string fmt, params object[] args);

        void Fatal(string msg, Exception ex = null);

        void FatalFmt(string fmt, params object[] args);
    }

    enum LoggingEvent
    {
        DEBUG, INFO, WARN, ERROR, FATAL
    }
}
