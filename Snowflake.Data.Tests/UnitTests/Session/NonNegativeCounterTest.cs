using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class NonNegativeCounterTest
    {
        [Test]
        public void TestInitialZero()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            var count = counter.Count();

            // assert
            Assert.AreEqual(0, count);
        }

        [Test]
        public void TestIncrease()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            counter.Increase();

            // assert
            Assert.AreEqual(1, counter.Count());

            // act
            counter.Increase();

            // assert
            Assert.AreEqual(2, counter.Count());
        }


        [Test]
        public void TestDecrease()
        {
            // arrange
            var counter = new NonNegativeCounter();
            counter.Increase();
            counter.Increase();

            // act
            counter.Decrease();

            // assert
            Assert.AreEqual(1, counter.Count());

            // act
            counter.Decrease();

            // assert
            Assert.AreEqual(0, counter.Count());
        }

        [Test]
        public void TestDecreaseDoesNotGoBelowZero()
        {
            // arrange
            var counter = new NonNegativeCounter();

            // act
            counter.Decrease();

            // assert
            Assert.AreEqual(0, counter.Count());
        }

        [Test]
        public void TestReset()
        {
            // arrange
            var counter = new NonNegativeCounter();
            counter.Increase();
            counter.Increase();

            // act
            counter.Reset();

            // assert
            Assert.AreEqual(0, counter.Count());
        }
    }
}
