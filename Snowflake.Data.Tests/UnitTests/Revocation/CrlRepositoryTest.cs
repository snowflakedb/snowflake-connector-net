using System;
using Xunit;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    public class CrlRepositoryTest
    {
        public void SetUp()
        {
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", null);
        }

        [Fact]
        public void TestDefaultCleanupInterval()
        {
            // arrange & act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(7), cleanupInterval);
        }

        [Fact]
        public void TestCustomCleanupIntervalFromEnvironmentVariable()
        {
            // arrange
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", "14");

            // act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(14), cleanupInterval);
        }

        [Fact]
        public void TestInvalidCleanupIntervalUsesDefault()
        {
            // arrange
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", "invalid");

            // act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(7), cleanupInterval);
        }
    }
}
