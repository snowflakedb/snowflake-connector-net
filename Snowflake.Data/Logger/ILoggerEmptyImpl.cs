/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using System;

namespace Snowflake.Data.Log
{
    // Empty implementation of SFLogger
    // Used when SFLoggerFactory.disableLogger() is called.

    class ILoggerEmptyImpl : ILogger
    {
        IDisposable ILogger.BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        bool ILogger.IsEnabled(LogLevel logLevel)
        {
            return false;
        }

        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            return;
        }
    }

}
