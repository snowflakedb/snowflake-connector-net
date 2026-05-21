using Xunit;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    public class NonNegativeCounterTest
    {
        [SFFact]
        public void TestInitialZero()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            var count = counter.Count();

            // assert
            Assert.Equal(0, count);
        }

        [SFFact]
        public void TestIncrease()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            counter.Increase();

            // assert
            Assert.Equal(1, counter.Count());

            // act
            counter.Increase();

            // assert
            Assert.Equal(2, counter.Count());
        }


        [SFFact]
        public void TestDecrease()
        {
            // arrange
            var counter = new NonNegativeCounter();
            counter.Increase();
            counter.Increase();

            // act
            counter.Decrease();

            // assert
            Assert.Equal(1, counter.Count());

            // act
            counter.Decrease();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [SFFact]
        public void TestDecreaseDoesNotGoBelowZero()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            counter.Decrease();

            // assert
            Assert.Equal(0, counter.Count());
        }

        [SFFact]
        public void TestReset()
        {
            // arrange
            var counter = new NonNegativeCounter();
            counter.Increase();
            counter.Increase();

            // act
            counter.Reset();

            // assert
            Assert.Equal(0, counter.Count());
        }
    }
}
