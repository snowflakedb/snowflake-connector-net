using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests;

public class RunOnlyOnCI : IgnoreOnEnvNotSetAttribute
{
    public RunOnlyOnCI() : base("CI")
    {
    }
}
