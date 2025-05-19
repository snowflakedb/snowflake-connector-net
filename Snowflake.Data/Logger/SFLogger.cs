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

        void Debug(string msg, Exception ex = null);

        void Info(string msg, Exception ex = null);

        void Warn(string msg, Exception ex = null);

        void Error(string msg, Exception ex = null);

        void Fatal(string msg, Exception ex = null);
    }

    enum LoggingEvent
    {
        DEBUG, INFO, WARN, ERROR, FATAL
    }

}
