namespace Snowflake.Data.Tests.Util;

public class IgnoreOnJenkins : IgnoreOnEnvIsSetAttribute
{
    public IgnoreOnJenkins() : base("JENKINS_HOME")
    {
    }
}
