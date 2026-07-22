using System;
using Moq;
using Snowflake.Data.Configuration;
using Xunit;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    public class CrlRepositoryTest
    {
        [SFFact]
        public void TestDefaultCleanupInterval()
        {
            // arrange
            var environmentOperations = new Mock<IEnvironmentFacade>();
            environmentOperations.Setup(e => e.GetInt(EnvVars.CrlCacheRemovalDelay)).Returns(7);
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var cleanupInterval = crlParser.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(7), cleanupInterval);
        }

        [SFFact]
        public void TestCustomCleanupIntervalFromEnvironmentVariable()
        {
            // arrange
            var environmentOperations = new Mock<IEnvironmentFacade>();
            environmentOperations.Setup(e => e.GetInt(EnvVars.CrlCacheRemovalDelay)).Returns(14);
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var cleanupInterval = crlParser.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(14), cleanupInterval);
        }

        [SFFact]
        public void TestInvalidCleanupIntervalUsesDefault()
        {
            // arrange - default value from EnvVars.CrlCacheRemovalDelay is 7
            var environmentOperations = new Mock<IEnvironmentFacade>();
            environmentOperations.Setup(e => e.GetInt(EnvVars.CrlCacheRemovalDelay)).Returns(7);
            var crlParser = new CrlParser(environmentOperations.Object);

            // act
            var cleanupInterval = crlParser.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(7), cleanupInterval);
        }
    }
}
