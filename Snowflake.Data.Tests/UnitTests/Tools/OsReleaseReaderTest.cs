using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public class OsReleaseReaderTest
    {
        [Test]
        public void TestParseFullOsRelease()
        {
            // arrange
            var contents =
                "NAME=\"Arch Linux\"\n" +
                "PRETTY_NAME=\"Arch Linux\"\n" +
                "ID=arch\n" +
                "BUILD_ID=rolling\n" +
                "VERSION_ID=20251019.0.436919\n" +
                "ANSI_COLOR=\"38;2;23;147;209\"\n" +
                "HOME_URL=\"https://archlinux.org/\"\n" +
                "DOCUMENTATION_URL=\"https://wiki.archlinux.org/\"\n" +
                "SUPPORT_URL=\"https://bbs.archlinux.org/\"\n" +
                "BUG_REPORT_URL=\"https://gitlab.archlinux.org/groups/archlinux/-/issues\"\n" +
                "PRIVACY_POLICY_URL=\"https://terms.archlinux.org/docs/privacy-policy/\"\n" +
                "LOGO=archlinux-logo\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual("Arch Linux", result["NAME"]);
            Assert.AreEqual("Arch Linux", result["PRETTY_NAME"]);
            Assert.AreEqual("arch", result["ID"]);
            Assert.AreEqual("rolling", result["BUILD_ID"]);
            Assert.AreEqual("20251019.0.436919", result["VERSION_ID"]);
            Assert.IsFalse(result.ContainsKey("ANSI_COLOR"));
            Assert.IsFalse(result.ContainsKey("HOME_URL"));
            Assert.IsFalse(result.ContainsKey("LOGO"));
        }

        [Test]
        public void TestParseUbuntuOsRelease()
        {
            // arrange
            var contents =
                "PRETTY_NAME=\"Ubuntu 22.04.3 LTS\"\n" +
                "NAME=\"Ubuntu\"\n" +
                "VERSION_ID=\"22.04\"\n" +
                "VERSION=\"22.04.3 LTS (Jammy Jellyfish)\"\n" +
                "ID=ubuntu\n" +
                "ID_LIKE=debian\n" +
                "HOME_URL=\"https://www.ubuntu.com/\"\n" +
                "SUPPORT_URL=\"https://help.ubuntu.com/\"\n" +
                "BUG_REPORT_URL=\"https://bugs.launchpad.net/ubuntu/\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual("Ubuntu 22.04.3 LTS", result["PRETTY_NAME"]);
            Assert.AreEqual("Ubuntu", result["NAME"]);
            Assert.AreEqual("22.04", result["VERSION_ID"]);
            Assert.AreEqual("22.04.3 LTS (Jammy Jellyfish)", result["VERSION"]);
            Assert.AreEqual("ubuntu", result["ID"]);
            Assert.IsFalse(result.ContainsKey("ID_LIKE"));
        }

        [Test]
        public void TestParseAllAllowedKeys()
        {
            // arrange
            var contents =
                "NAME=\"Test OS\"\n" +
                "PRETTY_NAME=\"Test OS 1.0\"\n" +
                "ID=testos\n" +
                "IMAGE_ID=test-image\n" +
                "IMAGE_VERSION=1.0.0\n" +
                "BUILD_ID=20250101\n" +
                "VERSION=\"1.0\"\n" +
                "VERSION_ID=\"1.0\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(8, result.Count);
            Assert.AreEqual("Test OS", result["NAME"]);
            Assert.AreEqual("Test OS 1.0", result["PRETTY_NAME"]);
            Assert.AreEqual("testos", result["ID"]);
            Assert.AreEqual("test-image", result["IMAGE_ID"]);
            Assert.AreEqual("1.0.0", result["IMAGE_VERSION"]);
            Assert.AreEqual("20250101", result["BUILD_ID"]);
            Assert.AreEqual("1.0", result["VERSION"]);
            Assert.AreEqual("1.0", result["VERSION_ID"]);
        }

        [Test]
        public void TestParseEmptyContent()
        {
            // act
            var result = OsReleaseReader.ParseOsReleaseContents("");

            // assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [Test]
        public void TestParseNullContent()
        {
            // act
            var result = OsReleaseReader.ParseOsReleaseContents(null);

            // assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [Test]
        public void TestParseIgnoresComments()
        {
            // arrange
            var contents =
                "# This is a comment\n" +
                "NAME=\"Test\"\n" +
                "# Another comment\n" +
                "ID=test\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Test", result["NAME"]);
            Assert.AreEqual("test", result["ID"]);
        }

        [Test]
        public void TestParseIgnoresEmptyLines()
        {
            // arrange
            var contents =
                "\n" +
                "NAME=\"Test\"\n" +
                "\n" +
                "ID=test\n" +
                "\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Test", result["NAME"]);
            Assert.AreEqual("test", result["ID"]);
        }

        [Test]
        public void TestParseIgnoresLowercaseKeys()
        {
            // arrange
            var contents =
                "name=\"Test\"\n" +
                "NAME=\"Valid\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("Valid", result["NAME"]);
        }

        [Test]
        public void TestParseQuotedAndUnquotedValues()
        {
            // arrange
            var contents =
                "NAME=\"Quoted Value\"\n" +
                "ID=unquoted\n" +
                "VERSION_ID=\"1.0\"\n" +
                "BUILD_ID=rolling\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(4, result.Count);
            Assert.AreEqual("Quoted Value", result["NAME"]);
            Assert.AreEqual("unquoted", result["ID"]);
            Assert.AreEqual("1.0", result["VERSION_ID"]);
            Assert.AreEqual("rolling", result["BUILD_ID"]);
        }

        [Test]
        public void TestParseEmptyQuotedValue()
        {
            // arrange
            var contents = "NAME=\"\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("", result["NAME"]);
        }

        [Test]
        public void TestParseOnlyDisallowedKeys()
        {
            // arrange
            var contents =
                "HOME_URL=\"https://example.com/\"\n" +
                "SUPPORT_URL=\"https://example.com/support\"\n" +
                "BUG_REPORT_URL=\"https://example.com/bugs\"\n" +
                "ANSI_COLOR=\"38;2;23;147;209\"\n" +
                "LOGO=mylogo\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(0, result.Count);
        }

        [Test]
        public void TestParseHandlesCarriageReturnLineFeed()
        {
            // arrange
            var contents = "NAME=\"Test\"\r\nID=test\r\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Test", result["NAME"]);
            Assert.AreEqual("test", result["ID"]);
        }
    }
}
