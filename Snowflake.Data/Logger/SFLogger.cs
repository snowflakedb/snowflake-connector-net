using System;

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
    }

    public enum LoggingEvent
    {
        OFF, TRACE, DEBUG, INFO, WARN, ERROR
    }
}
