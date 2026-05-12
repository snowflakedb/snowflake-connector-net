using System;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public class IgnoreOnEnvNotSetFactAttribute : FactAttribute
{
    public IgnoreOnEnvNotSetFactAttribute(string key)
    {
        var shouldSkip = string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key));
        if (shouldSkip)
            Skip = $"Test is ignored when environment variable {key} is not set.";
    }
}
