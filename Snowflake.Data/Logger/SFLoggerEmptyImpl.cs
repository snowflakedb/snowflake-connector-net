/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;

namespace Snowflake.Data.Log
{
    // Empty implementation of SFLogger
    // Used when SFLoggerFactory.disableLogger() is called.

    class SFLoggerEmptyImpl : SFLogger
    {
        public bool IsDebugEnabled()
        {
            return false;
        }

        public bool IsInfoEnabled()
        {
            return false;
        }

        public bool IsWarnEnabled()
        {
            return false;
        }

        public bool IsErrorEnabled()
        {
            return false;
        }

        public bool IsFatalEnabled()
        {
            return false;
        }

        public void Debug(string msg, Exception ex)
        {
            return;
        }

        public void Info(string msg, Exception ex)
        {
            return;
        }

        public void Warn(string msg, Exception ex)
        {
            return;
        }

        public void Error(string msg, Exception ex)
        {
            return;
        }

        public void Fatal(string msg, Exception ex)
        {
            return;
        }

        List<SFAppender> SFLogger.GetAppenders()
        {
            throw new NotImplementedException();
        }

        void SFLogger.AddAppender(SFAppender appender)
        {
            throw new NotImplementedException();
        }

        void SFLogger.RemoveAppender(SFAppender appender)
        {
            throw new NotImplementedException();
        }

        void SFLogger.SetLevel(LoggingEvent level)
        {
            throw new NotImplementedException();
        }
    }

}
