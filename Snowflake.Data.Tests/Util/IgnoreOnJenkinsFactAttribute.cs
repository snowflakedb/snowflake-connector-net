namespace Snowflake.Data.Tests.Util;

public class IgnoreOnJenkinsFactAttribute : IgnoreOnEnvIsSetFactAttribute
{
    public IgnoreOnJenkinsFactAttribute() : base("JENKINS_HOME")
    {
    }
}

public class IgnoreOnJenkinsTheoryAttribute : IgnoreOnEnvIsSetTheoryAttribute
{
    public IgnoreOnJenkinsTheoryAttribute() : base("JENKINS_HOME")
    {
    }
}
