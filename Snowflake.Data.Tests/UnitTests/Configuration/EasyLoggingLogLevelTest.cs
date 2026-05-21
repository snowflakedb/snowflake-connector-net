using System;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    public class EasyLoggingLogLevelTest
    {
        [SFTheory]
        [InlineData("OFF", 0)]
        [InlineData("off", 0)]
        [InlineData("iNfO", 3)]
        public void TestThatGetsLogLevelValueIgnoringLetterCase(string loglevelString, int expectedLogLevel)
        {
            // act
            var logLevel = EasyLoggingLogLevelExtensions.From(loglevelString);

            // assert
            Assert.Equal((EasyLoggingLogLevel)expectedLogLevel, logLevel);
        }

        [SFFact]
        public void TestThatFailsToParseLogLevelFromUnknownValue()
        {
            // act
            var thrown = Assert.Throws<ArgumentException>(() => EasyLoggingLogLevelExtensions.From("unknown"));

            // assert
            Assert.NotNull(thrown);
            Assert.Equal("Requested value 'unknown' was not found.", thrown.Message);
        }
    }
}
