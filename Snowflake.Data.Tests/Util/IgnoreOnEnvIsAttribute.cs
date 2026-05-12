using System;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public class IgnoreOnEnvIsAttribute : FactAttribute
{
    public IgnoreOnEnvIsAttribute(string key, string[] values, string reason = null)
    {
        foreach (var value in values)
        {
            var shouldSkip = Environment.GetEnvironmentVariable(key) == value;
            if (shouldSkip)
                Skip = $"Test is ignored when environment variable {key} is {value}. {reason}";
        }
    }
}
