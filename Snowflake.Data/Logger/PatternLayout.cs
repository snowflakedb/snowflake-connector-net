using System;
using System.Threading;

internal class PatternLayout
{
    internal string ConversionPattern { get; set; }

    public PatternLayout() { }

    public string Format(string logLevel, string message, Type type)
    {
        var formattedMessage = ConversionPattern
            .Replace("%date", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
            .Replace("%t", Thread.CurrentThread.ManagedThreadId.ToString())
            .Replace("%-5level", logLevel)
            .Replace("%logger", type.ToString())
            .Replace("%message", message)
            .Replace("%newline", "\n");

        return formattedMessage;
    }
}
