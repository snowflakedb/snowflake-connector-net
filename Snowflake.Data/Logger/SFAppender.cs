using System;

namespace Snowflake.Data.Log
{
    internal interface SFAppender
    {
        void Append(string logLevel, string message, Type type, Exception ex = null);

        void ActivateOptions();
    }
}
