using System;

namespace Snowflake.Data.Log
{
    internal interface SFLogger
    {
        bool IsTraceEnabled();

        bool IsDebugEnabled();

        bool IsInfoEnabled();

        bool IsWarnEnabled();

        bool IsErrorEnabled();

        void Trace(string msg, Exception ex = null);

        void Debug(string msg, Exception ex = null);

        void Info(string msg, Exception ex = null);

        void Warn(string msg, Exception ex = null);

        void Error(string msg, Exception ex = null);
    }

    internal enum LoggingEvent
    {
        OFF, TRACE, DEBUG, INFO, WARN, ERROR
    }
}
