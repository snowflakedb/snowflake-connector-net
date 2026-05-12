namespace Snowflake.Data.Tests.Util;

public class IgnoreOnCI : IgnoreOnEnvIsAttribute
{
    public IgnoreOnCI(string reason = null) : base("CI", new[] { "true" }, reason)
    {
    }
}
