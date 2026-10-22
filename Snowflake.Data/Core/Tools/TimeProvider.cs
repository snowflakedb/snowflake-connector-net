using System;

namespace Snowflake.Data.Core.Tools;

internal class TimeProvider
{
    public static TimeProvider Instance = new();

    public virtual DateTime UtcNow() => DateTime.UtcNow;
}
