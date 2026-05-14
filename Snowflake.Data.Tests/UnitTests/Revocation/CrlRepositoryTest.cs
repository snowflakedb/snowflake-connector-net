using System;
using Xunit;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    public class CrlRepositoryTest
    {
        public CrlRepositoryTest()
        {
            SetUp();
        }

        public void SetUp()
        {
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", null);
        }

        [SFFact]
        public void TestDefaultCleanupInterval()
        {
            // arrange & act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(7), cleanupInterval);
        }

        [SFFact]
        public void TestCustomCleanupIntervalFromEnvironmentVariable()
        {
            // arrange
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", "14");

            // act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.Equal(TimeSpan.FromDays(14), cleanupInterval);
        }

        [SFFact]
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
