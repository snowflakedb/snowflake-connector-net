using System;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class CodeVerifierTest
    {
        [Test]
        public void TestFailForTooShortCodeVerifier()
        {
            // act
            var thrown = Assert.Throws<ArgumentException>(() => new CodeVerifier("shortText"));

            // assert
            Assert.That(thrown.Message, Does.Contain("The code verifier must be at least 43 characters"));
        }

        [Test]
        public void TestFailForTooLongCodeVerifier()
        {
            // arrange
            var longString = Enumerable.Range(1, 15)
                .Select(x => "0123456789")
                .Aggregate(string.Concat);

            // act
            var thrown = Assert.Throws<ArgumentException>(() => new CodeVerifier(longString));

            // assert
            Assert.That(thrown.Message, Does.Contain("The code verifier must not be longer than 128 characters"));
        }

        [Test]
        public void TestSkipIllegalCharacters()
        {
            // arrange
            var codeVerifier = Enumerable.Range(1, 5)
                .Select(x => "0123456789")
                .Aggregate(string.Concat);
            var illegalCharacters = "!@%";

            // act
            var result = new CodeVerifier(codeVerifier + illegalCharacters);

            // assert
            Assert.AreEqual(codeVerifier, result.Value);
        }
    }
}
