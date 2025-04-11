using System;
using System.Collections.Generic;

namespace Snowflake.Data.Log
{
    internal class SFLoggerImpl : SFLogger
    {
        private readonly Type _type;
        internal static List<SFAppender> s_appenders = new List<SFAppender>();
        internal static LoggingEvent s_level = LoggingEvent.OFF;

        private static bool s_isDebugEnabled = false;
        private static bool s_isInfoEnabled = false;
        private static bool s_isWarnEnabled = false;
        private static bool s_isErrorEnabled = false;

        internal SFLoggerImpl(Type type)
        {
            _type = type;
        }

        internal static void SetLevel(LoggingEvent level)
        {
            s_level = level;
            SetEnableValues();
        }

        private static void SetEnableValues()
        {
            var enabled = s_level != LoggingEvent.OFF;
            s_isDebugEnabled = enabled;
            s_isInfoEnabled = enabled;
            s_isWarnEnabled = enabled;
            s_isErrorEnabled = enabled;

            if (enabled)
            {
                switch (s_level)
                {
                    case LoggingEvent.TRACE:
                    case LoggingEvent.DEBUG:
                        break;
                    case LoggingEvent.ERROR:
                        s_isWarnEnabled = false;
                        s_isInfoEnabled = false;
                        s_isDebugEnabled = false;
                        break;
                    case LoggingEvent.WARN:
                        s_isInfoEnabled = false;
                        s_isDebugEnabled = false;
                        break;
                    case LoggingEvent.INFO:
                        s_isDebugEnabled = false;
                        break;
                }
            }
        }

        public bool IsDebugEnabled()
        {
            return s_isDebugEnabled;
        }

        public bool IsInfoEnabled()
        {
            return s_isInfoEnabled;
        }

        public bool IsWarnEnabled()
        {
            return s_isWarnEnabled;
        }

        public bool IsErrorEnabled()
        {
            return s_isErrorEnabled;
        }

        public void Debug(string msg, Exception ex = null)
        {
            if (IsDebugEnabled())
            {
                Log(LoggingEvent.DEBUG.ToString(), msg, ex);
            }
        }

        public void Info(string msg, Exception ex = null)
        {
            if (IsInfoEnabled())
            {
                Log(LoggingEvent.INFO.ToString(), msg, ex);
            }
        }

        public void Warn(string msg, Exception ex = null)
        {
            if (IsWarnEnabled())
            {
                Log(LoggingEvent.WARN.ToString(), msg, ex);
            }
        }


        public void Error(string msg, Exception ex = null)
        {
            if (IsErrorEnabled())
            {
                Log(LoggingEvent.ERROR.ToString(), msg, ex);
            }
        }

        private void Log(string logLevel, string logMessage, Exception ex = null)
        {
            if (s_appenders.Count > 0)
            {
                foreach (var appender in s_appenders)
                {
                    appender.Append(logLevel, logMessage, _type, ex);
                }
            }
        }
    }
}
