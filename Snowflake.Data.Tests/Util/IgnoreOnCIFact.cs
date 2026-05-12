namespace Snowflake.Data.Tests.Util;

public class IgnoreOnCIFact : IgnoreOnEnvIsAttribute
{
    public IgnoreOnCIFact(string reason = null) : base("CI", new[] { "true" }, reason)
    {
    }
}
