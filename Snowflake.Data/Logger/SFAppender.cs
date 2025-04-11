using System;

namespace Snowflake.Data.Log
{
    internal interface SFAppender
    {
        string Name { get; }

        void Append(string logLevel, string message, Type type, Exception ex = null);
    }
}
