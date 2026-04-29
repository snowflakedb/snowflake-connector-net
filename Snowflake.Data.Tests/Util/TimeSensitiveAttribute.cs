using System;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;
using Snowflake.Data.Tests.IntegrationTests;

namespace Snowflake.Data.Tests.Util
{
    /// <summary>
    /// Marks a test as time-sensitive (e.g. uses Stopwatch to assert timing).
    /// These tests are run in isolation, so other tests running in parallel won't interfere too much with them.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class TimeSensitiveAttribute : Attribute, IApplyToTest
    {
        private readonly string _extraInfo;

        public TimeSensitiveAttribute(string extraInfo)
        {
            _extraInfo = extraInfo;
        }

        public TimeSensitiveAttribute() : this(string.Empty) { }

        public void ApplyToTest(Test test)
        {
            test.RunState = RunState.Ignored;
            var extraInfoText = string.IsNullOrEmpty(_extraInfo) ? string.Empty : $"Extra info: {_extraInfo}";
            test.Properties.Set(PropertyNames.SkipReason, $"This test is run separately, due to time dependencies. Look for {nameof(TimeSensitiveFixtureIT)} {extraInfoText}.");
        }
    }
}
