using Snowflake.Data.Log;
using System;

internal class SFConsoleAppender : SFAppender
{
    internal PatternLayout _patternLayout;

    public SFConsoleAppender() { }

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = _patternLayout.Format(logLevel, message, type);
        Console.Write(formattedMessage);
        if (ex != null)
        {
            Console.WriteLine(ex.Message);
        }
    }
}
