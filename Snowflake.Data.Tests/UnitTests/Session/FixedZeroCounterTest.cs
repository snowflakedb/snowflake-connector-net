using Xunit;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public class FixedZeroCounterTest
    {
        [Fact]
        public void TestInitialZero()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            var count = counter.Count();

            // assert
            Assert.Equal(0, count);
        }

        [Fact]
        public void TestZeroAfterIncrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Increase();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [Fact]
        public void TestZeroAfterDecrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Decrease();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [Fact]
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
