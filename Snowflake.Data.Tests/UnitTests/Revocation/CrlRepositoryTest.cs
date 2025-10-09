using System;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlRepositoryTest
    {
        [SetUp]
        public void SetUp()
        {
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", null);
        }

        [Test]
        public void TestDefaultCleanupInterval()
        {
            // arrange & act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.AreEqual(TimeSpan.FromDays(7), cleanupInterval,
                "Default cleanup interval should be 7 days");
        }

        [Test]
        public void TestCustomCleanupIntervalFromEnvironmentVariable()
        {
            // arrange
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", "14");

            // act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.AreEqual(TimeSpan.FromDays(14), cleanupInterval,
                "Cleanup interval should be 14 days when SF_CRL_CACHE_REMOVAL_DELAY=14");
        }

        [Test]
        public void TestInvalidCleanupIntervalUsesDefault()
        {
            // arrange
            Environment.SetEnvironmentVariable("SF_CRL_CACHE_REMOVAL_DELAY", "invalid");

            // act
            var cleanupInterval = CrlRepository.GetCleanupInterval();

            // assert
            Assert.AreEqual(TimeSpan.FromDays(7), cleanupInterval,
                "Should use default 7 days when environment variable is invalid");
        }
    }
}
