using System;
using NUnit.Framework;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    class EasyLoggingLogLevelTest
    {
        [Test]
        [TestCase("OFF", EasyLoggingLogLevel.OFF)]
        [TestCase("off", EasyLoggingLogLevel.OFF)]
        [TestCase("iNfO", EasyLoggingLogLevel.INFO)]
        public void TestThatGetsLogLevelValueIgnoringLetterCase(string loglevelString, EasyLoggingLogLevel expectedLogLevel)
        {
            // act
            var logLevel = EasyLoggingLogLevelExtensions.From(loglevelString);
            
            // assert
            Assert.AreEqual(expectedLogLevel, logLevel);
        }

        [Test]
        public void TestThatFailsToParseLogLevelFromUnknownValue()
        {
            // act
            var thrown = Assert.Throws<ArgumentException>(() => EasyLoggingLogLevelExtensions.From("unknown"));
            
            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual("Requested value 'unknown' was not found.", thrown.Message);
        }
    }
}
