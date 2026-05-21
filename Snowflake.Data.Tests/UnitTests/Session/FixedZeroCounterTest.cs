using Xunit;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class FixedZeroCounterTest
    {
        [Test]
        public void TestInitialZero()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            var count = counter.Count();

            // assert
            Assert.Equal(0, count);
        }

        [Test]
        public void TestZeroAfterIncrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Increase();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [Test]
        public void TestZeroAfterDecrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Decrease();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [Test]
        public void TestZeroAfterReset()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Reset();

            // assert
            Assert.Equal(0, counter.Count());
        }
    }
}
