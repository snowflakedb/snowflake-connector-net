using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public sealed class LibcDetectorTest
    {
        private static readonly LibcDetector s_libcDetector = (LibcDetector)LibcDetector.Instance;

        [Test]
        public void TestGetLibcFamilyStringForGlibc()
        {
            // Act
            var result = LibcFamily.Glibc.ToPrettyString();

            // Assert
            Assert.That(result, Is.EqualTo("glibc"));
        }

        [Test]
        public void TestGetLibcFamilyStringForMusl()
        {
            // Act
            var result = LibcFamily.Musl.ToPrettyString();

            // Assert
            Assert.That(result, Is.EqualTo("musl"));
        }

        [Test]
        public void TestGetLibcFamilyStringForNotApplicable()
        {
            // Act
            var result = LibcFamily.NotApplicable.ToPrettyString();

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestGetLibcFamilyStringForCouldNotDetermine()
        {
            // Act
            var result = LibcFamily.CouldNotDetermine.ToPrettyString();

            // Assert
            Assert.That(result, Is.EqualTo("could not determine"));
        }

        [Test]
        public void TestParseMuslVersionFromTypicalOutput()
        {
            // Arrange
            var output =
                "musl libc (x86_64)\n" +
                "Version 1.2.5\n" +
                "Dynamic Program Loader\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.EqualTo("1.2.5"));
        }

        [Test]
        public void TestParseMuslVersionRequiresLowercaseMuslKeyword()
        {
            // Arrange — uppercase "MUSL" is not matched (real musl output uses lowercase)
            var output =
                "MUSL libc (x86_64)\n" +
                "Version 1.2.3\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestParseMuslVersionCaseInsensitiveVersionPrefix()
        {
            // Arrange
            var output =
                "musl libc (aarch64)\n" +
                "version 1.1.24\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.EqualTo("1.1.24"));
        }

        [Test]
        public void TestParseMuslVersionReturnsNullWhenNoMuslKeyword()
        {
            // Arrange — glibc ldd output
            var output =
                "ldd (Ubuntu GLIBC 2.35-0ubuntu3.8) 2.35\n" +
                "Copyright (C) 2022 Free Software Foundation, Inc.\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestParseMuslVersionReturnsNullWhenMuslPresentButNoVersionLine()
        {
            // Arrange
            var output = "musl libc (x86_64)\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestParseMuslVersionReturnsNullForNullInput()
        {
            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(null);

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestParseMuslVersionReturnsNullForEmptyInput()
        {
            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput("");

            // Assert
            Assert.That(result, Is.Null);
        }

        [Test]
        public void TestParseMuslVersionTrimsWhitespaceFromVersion()
        {
            // Arrange
            var output =
                "musl libc\n" +
                "  Version  1.2.4  \n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.EqualTo("1.2.4"));
        }

        [Test]
        public void TestParseMuslVersionHandlesCarriageReturnLineFeed()
        {
            // Arrange
            var output = "musl libc (x86_64)\r\nVersion 1.2.5\r\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.EqualTo("1.2.5"));
        }

        [Test]
        public void TestParseMuslVersionReturnsFirstVersionLineFound()
        {
            // Arrange
            var output =
                "musl libc\n" +
                "Version 1.2.3\n" +
                "Version 9.9.9\n";

            // Act
            var result = s_libcDetector.ParseMuslVersionFromLddOutput(output);

            // Assert
            Assert.That(result, Is.EqualTo("1.2.3"));
        }
    }
}
