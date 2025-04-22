using Snowflake.Data.Log;
using System;

internal class SFConsoleAppender : SFAppender
{
    internal PatternLayout PatternLayout { get; set; }

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = PatternLayout.Format(logLevel, message, type);
        Console.Write(formattedMessage);
        if (ex != null)
        {
            Console.WriteLine(ex.Message);
        }
    }
}
