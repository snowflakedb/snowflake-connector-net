/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using log4net;
using System;

namespace Snowflake.Data.Log
{
    class Log4netImpl : SFLogger
    {
        private readonly ILog logger;

        public Log4netImpl(ILog logger)
        {
            this.logger = logger;
        }

        public bool IsDebugEnabled()
        {
            return logger.IsDebugEnabled;
        }

        public bool IsInfoEnabled()
        {
            return logger.IsInfoEnabled;
        }

        public bool IsWarnEnabled()
        {
            return logger.IsWarnEnabled;
        }

        public bool IsErrorEnabled()
        {
            return logger.IsErrorEnabled;
        }

        public bool IsFatalEnabled()
        {
            return logger.IsFatalEnabled;
        }

        public void Debug(string msg, Exception ex = null)
        {
            logger.Debug(msg, ex);
        }

        public void DebugFmt(string fmt, params object[] args)
        {
            logger.DebugFormat(fmt, args);
        }

        public void Info(string msg, Exception ex = null)
        {
            logger.Info(msg, ex);
        }

        public void InfoFmt(string fmt, params object[] args)
        {
            logger.InfoFormat(fmt, args);
        }

        public void Warn(string msg, Exception ex = null)
        {
            logger.Warn(msg, ex);
        }

        public void WarnFmt(string fmt, params object[] args)
        {
            logger.WarnFormat(fmt, args);
        }

        public void Error(string msg, Exception ex = null)
        {
            logger.Error(msg, ex);
        }

        public void ErrorFmt(string fmt, params object[] args)
        {
            logger.InfoFormat(fmt, args);
        }

        public void Fatal(string msg, Exception ex = null)
        {
            logger.Fatal(msg, ex);
        }

        public void FatalFmt(string fmt, params object[] args)
        {
            logger.InfoFormat(fmt, args);
        }
    }
}
