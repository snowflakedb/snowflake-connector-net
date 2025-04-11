using System;
using System.Collections.Generic;

namespace Snowflake.Data.Log
{
    internal class SFLoggerImpl : SFLogger
    {
        private readonly Type _type;
        internal static List<SFAppender> s_appenders = new List<SFAppender>();
        internal static LoggingEvent s_level = LoggingEvent.OFF;

        private static bool _isDebugEnabled = false;
        private static bool _isInfoEnabled = false;
        private static bool _isWarnEnabled = false;
        private static bool _isErrorEnabled = false;

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
            _isDebugEnabled = enabled;
            _isInfoEnabled = enabled;
            _isWarnEnabled = enabled;
            _isErrorEnabled = enabled;

            if (enabled)
            {
                switch (s_level)
                {
                    case LoggingEvent.TRACE:
                    case LoggingEvent.DEBUG:
                        break;
                    case LoggingEvent.ERROR:
                        _isWarnEnabled = false;
                        _isInfoEnabled = false;
                        _isDebugEnabled = false;
                        break;
                    case LoggingEvent.WARN:
                        _isInfoEnabled = false;
                        _isDebugEnabled = false;
                        break;
                    case LoggingEvent.INFO:
                        _isDebugEnabled = false;
                        break;
                }
            }
        }

        public bool IsDebugEnabled()
        {
            return _isDebugEnabled;
        }

        public bool IsInfoEnabled()
        {
            return _isInfoEnabled;
        }

        public bool IsWarnEnabled()
        {
            return _isWarnEnabled;
        }

        public bool IsErrorEnabled()
        {
            return _isErrorEnabled;
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
