using Xunit;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public class FixedZeroCounterTest
    {
        [SFFact]
        public void TestInitialZero()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            var count = counter.Count();

            // assert
            Assert.Equal(0, count);
        }

        [SFFact]
        public void TestZeroAfterIncrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Increase();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [SFFact]
        public void TestZeroAfterDecrease()
        {
            // arrange
            var counter = new FixedZeroCounter();

            // act
            counter.Decrease();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [SFFact]
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
