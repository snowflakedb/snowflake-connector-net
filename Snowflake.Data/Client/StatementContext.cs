using System.IO;

namespace Snowflake.Data.Client;

internal sealed record StatementContext(
    MemoryStream Stream,
    bool DescribeOnly,
    bool AsyncExec,
    string CommandText,
    int CommandTimeout,
    string QueryTag)
{
    public static StatementContext Default { get; } = new(null, false, false, null, 0, null);
};

