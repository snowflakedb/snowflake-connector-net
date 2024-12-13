using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public class DirectoryInformationTest
    {
        [Test]
        [TestCaseSource(nameof(OldCreatingDatesTestCases))]
        public void TestIsCreatedEarlierThanSeconds(DateTime? createdDate, DateTime utcNow)
        {
            // arrange
            var directoryInformation = new DirectoryInformation(true, createdDate);

            // act
            var result = directoryInformation.IsCreatedEarlierThanSeconds(60, utcNow);

            // assert
            Assert.AreEqual(true, result);
        }

        [Test]
        [TestCaseSource(nameof(NewCreatingDatesTestCases))]
        public void TestIsNotCreatedEarlierThanSeconds(bool dirExists, DateTime? createdDate, DateTime utcNow)
        {
            // arrange
            var directoryInformation = new DirectoryInformation(dirExists, createdDate);

            // act
            var result = directoryInformation.IsCreatedEarlierThanSeconds(60, utcNow);

            // assert
            Assert.AreEqual(false, result);
        }

        internal static IEnumerable<object[]> OldCreatingDatesTestCases()
        {
            yield return new object[] { DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow };
            yield return new object[] { DateTime.UtcNow.AddSeconds(-61), DateTime.UtcNow };
        }

        internal static IEnumerable<object[]> NewCreatingDatesTestCases()
        {
            yield return new object[] { true, DateTime.UtcNow.AddSeconds(-30), DateTime.UtcNow };
            yield return new object[] { true, DateTime.UtcNow.AddSeconds(30), DateTime.UtcNow };
            yield return new object[] { true, DateTime.UtcNow, DateTime.UtcNow };
            yield return new object[] { false, null, DateTime.UtcNow };
        }
    }
}
