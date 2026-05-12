using System;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public class IgnoreOnEnvIsSetFactAttribute : FactAttribute
{
    public IgnoreOnEnvIsSetFactAttribute(string key)
    {
        var shouldSkip = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key));
        if (shouldSkip)
            Skip = $"Test is ignored when environment variable {key} is set.";
    }
}

public class IgnoreOnEnvIsSetTheoryAttribute : TheoryAttribute
{
    public IgnoreOnEnvIsSetTheoryAttribute(string key)
    {
        var shouldSkip = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key));
        if (shouldSkip)
            Skip = $"Test is ignored when environment variable {key} is set.";
    }
}

