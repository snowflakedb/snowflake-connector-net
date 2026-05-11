using System.Runtime.CompilerServices;
using Xunit;

namespace Snowflake.Data.Tests.Util;

[Xunit.Sdk.XunitTestCaseDiscoverer("Xunit.Sdk.TheoryDiscoverer", "xunit.execution.{Platform}")]
public sealed class FactWithAlteredDisplayNameAttribute : FactAttribute
{
    public FactWithAlteredDisplayNameAttribute(string suffix = "_",
        string replacementChars = " ",
        [CallerMemberName] string testMethodName = "")
    {
        DisplayName = testMethodName + suffix;
    }
}
