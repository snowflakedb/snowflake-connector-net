using System;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public class IgnoreOnEnvIsSetAttribute : FactAttribute
{
    public IgnoreOnEnvIsSetAttribute(string key)
    {
        var shouldSkip = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key));
        if (shouldSkip)
            Skip = $"Test is ignored when environment variable {key} is set.";
    }
}
