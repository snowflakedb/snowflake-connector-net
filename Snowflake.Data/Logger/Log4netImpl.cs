using log4net;
using System;

namespace Snowflake.Data.Log
{
    // Default implementation for SFLogger

    class Log4NetImpl : SFLogger
    {
        private readonly ILog logger;

        public Log4NetImpl(ILog logger)
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
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            logger.Debug(msg, ex);
        }

        public void Info(string msg, Exception ex = null)
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            logger.Info(msg, ex);
        }

        public void Warn(string msg, Exception ex = null)
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            logger.Warn(msg, ex);
        }


        public void Error(string msg, Exception ex = null)
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            logger.Error(msg, ex);
        }

        public void Fatal(string msg, Exception ex = null)
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            logger.Fatal(msg, ex);
        }
    }

}
