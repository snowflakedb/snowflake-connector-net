using System;
using Xunit;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{

    class EasyLoggingLogLevelTest
    {
        [Test]
        [TestCase("OFF", EasyLoggingLogLevel.Off)]
        [TestCase("off", EasyLoggingLogLevel.Off)]
        [TestCase("iNfO", EasyLoggingLogLevel.Info)]
        public void TestThatGetsLogLevelValueIgnoringLetterCase(string loglevelString, EasyLoggingLogLevel expectedLogLevel)
        {
            // act
            var logLevel = EasyLoggingLogLevelExtensions.From(loglevelString);

            // assert
            Assert.Equal(expectedLogLevel, logLevel);
        }

        [Test]
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
