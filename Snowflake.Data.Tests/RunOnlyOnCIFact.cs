using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests;

public class RunOnlyOnCIFact : IgnoreOnEnvNotSetFactAttribute
{
    public RunOnlyOnCIFact() : base("CI")
    {
    }
}
