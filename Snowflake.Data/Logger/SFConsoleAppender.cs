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
            catch (Exception)
            {
                try
                {
                    Console.Error.WriteLine("Encountered an error while writing to console");
                }
                catch (Exception)
                {
                }
            }
        }
    }
}
