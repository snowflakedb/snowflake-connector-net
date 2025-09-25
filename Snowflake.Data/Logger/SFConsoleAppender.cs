using System;

namespace Snowflake.Data.Log
{
    internal class SFConsoleAppender : SFAppender
    {
        internal PatternLayout PatternLayout { get; set; }

        public void Append(string logLevel, string message, Type type, Exception ex = null)
        {
            var formattedMessage = PatternLayout.Format(logLevel, message, type);
            try
            {
                Console.Write(formattedMessage);
                if (ex != null)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
            catch (Exception consoleEx)
            {
                Console.WriteLine($"An error occured while logging to console: {consoleEx.Message}");
            }
        }
    }
}
