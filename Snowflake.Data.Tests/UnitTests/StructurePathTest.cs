using NUnit.Framework;
using Snowflake.Data.Core.Converter;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class StructurePathTest
    {
        [Test]
        public void TestRootPath()
        {
            // act
            var value = new StructurePath().ToString();

            // assert
            Assert.AreEqual("$", value);
        }

        [Test]
        public void TestAddPropertyIndex()
        {
            // arrange
            var path = new StructurePath();

            // act
            var value = path.WithPropertyIndex(2);

            // assert
            Assert.AreEqual("$[2]", value.ToString());
        }

        [Test]
        public void TestAddPropertyIndexToComplexPath()
        {
            // arrange
            var path = new StructurePath().WithPropertyIndex(2);

            // act
            var value = path.WithPropertyIndex(1);

            // assert
            Assert.AreEqual("$[2][1]", value.ToString());
        }

        [Test]
        public void TestAddArrayIndex()
        {
            // arrange
            var path = new StructurePath();

            // act
            var value = path.WithArrayIndex(2);

            // assert
            Assert.AreEqual("$[2]", value.ToString());
        }

        [Test]
        public void TestAddArrayIndexToComplexPath()
        {
            // arrange
            var path = new StructurePath().WithArrayIndex(2);

            // act
            var value = path.WithArrayIndex(1);

            // assert
            Assert.AreEqual("$[2][1]", value.ToString());
        }

        [Test]
        public void TestAddMapIndex()
        {
            // arrange
            var path = new StructurePath();

            // act
            var value = path.WithMapIndex(2);

            // assert
            Assert.AreEqual("$[2]", value.ToString());
        }

        [Test]
        public void TestAddMapIndexToComplexPath()
        {
            // arrange
            var path = new StructurePath().WithMapIndex(2);

            // act
            var value = path.WithMapIndex(1);

            // assert
            Assert.AreEqual("$[2][1]", value.ToString());
        }
    }
}
