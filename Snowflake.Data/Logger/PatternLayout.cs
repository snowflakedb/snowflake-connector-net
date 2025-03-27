/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;

internal class PatternLayout
{
    internal string _conversionPattern;

    public PatternLayout() { }

    public string Format(string logLevel, string message, Type type)
    {
        var formattedMessage = _conversionPattern
            .Replace("%date", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
            .Replace("%t", Thread.CurrentThread.ManagedThreadId.ToString())
            .Replace("%-5level", logLevel)
            .Replace("%logger", type.ToString())
            .Replace("%message", message)
            .Replace("%newline", "\n");

        return formattedMessage;
    }
}
