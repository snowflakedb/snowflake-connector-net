using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class OsReleaseReaderTest
    {
        [SFFact]
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
            Assert.Equal(5, result.Count);
            Assert.Equal("Arch Linux", result["NAME"]);
            Assert.Equal("Arch Linux", result["PRETTY_NAME"]);
            Assert.Equal("arch", result["ID"]);
            Assert.Equal("rolling", result["BUILD_ID"]);
            Assert.Equal("20251019.0.436919", result["VERSION_ID"]);
            Assert.False(result.ContainsKey("ANSI_COLOR"));
            Assert.False(result.ContainsKey("HOME_URL"));
            Assert.False(result.ContainsKey("LOGO"));
        }

        [SFFact]
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
            Assert.Equal(5, result.Count);
            Assert.Equal("Ubuntu 22.04.3 LTS", result["PRETTY_NAME"]);
            Assert.Equal("Ubuntu", result["NAME"]);
            Assert.Equal("22.04", result["VERSION_ID"]);
            Assert.Equal("22.04.3 LTS (Jammy Jellyfish)", result["VERSION"]);
            Assert.Equal("ubuntu", result["ID"]);
            Assert.False(result.ContainsKey("ID_LIKE"));
        }

        [SFFact]
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
            Assert.Equal(8, result.Count);
            Assert.Equal("Test OS", result["NAME"]);
            Assert.Equal("Test OS 1.0", result["PRETTY_NAME"]);
            Assert.Equal("testos", result["ID"]);
            Assert.Equal("test-image", result["IMAGE_ID"]);
            Assert.Equal("1.0.0", result["IMAGE_VERSION"]);
            Assert.Equal("20250101", result["BUILD_ID"]);
            Assert.Equal("1.0", result["VERSION"]);
            Assert.Equal("1.0", result["VERSION_ID"]);
        }

        [SFFact]
        public void TestParseEmptyContent()
        {
            // act
            var result = OsReleaseReader.ParseOsReleaseContents("");

            // assert
            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [SFFact]
        public void TestParseNullContent()
        {
            // act
            var result = OsReleaseReader.ParseOsReleaseContents(null);

            // assert
            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [SFFact]
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
            Assert.Equal(2, result.Count);
            Assert.Equal("Test", result["NAME"]);
            Assert.Equal("test", result["ID"]);
        }

        [SFFact]
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
            Assert.Equal(2, result.Count);
            Assert.Equal("Test", result["NAME"]);
            Assert.Equal("test", result["ID"]);
        }

        [SFFact]
        public void TestParseIgnoresLowercaseKeys()
        {
            // arrange
            var contents =
                "name=\"Test\"\n" +
                "NAME=\"Valid\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.Single(result);
            Assert.Equal("Valid", result["NAME"]);
        }

        [SFFact]
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
            Assert.Equal(4, result.Count);
            Assert.Equal("Quoted Value", result["NAME"]);
            Assert.Equal("unquoted", result["ID"]);
            Assert.Equal("1.0", result["VERSION_ID"]);
            Assert.Equal("rolling", result["BUILD_ID"]);
        }

        [SFFact]
        public void TestParseEmptyQuotedValue()
        {
            // arrange
            var contents = "NAME=\"\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.Single(result);
            Assert.Equal("", result["NAME"]);
        }

        [SFFact]
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
            Assert.Empty(result);
        }

        [SFFact]
        public void TestParseIgnoresPartiallyMatchingKeys()
        {
            // arrange
            var contents =
                "NAME=\"Valid\"\n" +
                "NAMES=\"Invalid\"\n" +
                "USERNAME=\"root\"\n" +
                "USER_ID=\"1000\"\n" +
                "ID=valid\n" +
                "ID_LIKE=debian\n" +
                "VERSION_ID=\"1.0\"\n" +
                "VERSION_IDENTIFIER=\"nope\"\n" +
                "BUILD_ID=valid\n" +
                "BUILD_IDS=invalid\n" +
                "IMAGE_ID=valid\n" +
                "IMAGE_IDENTIFIER=invalid\n" +
                "PRETTY_NAME=\"Valid\"\n" +
                "PRETTY_NAMES=\"Invalid\"\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.Equal(6, result.Count);
            Assert.Equal("Valid", result["NAME"]);
            Assert.Equal("valid", result["ID"]);
            Assert.Equal("1.0", result["VERSION_ID"]);
            Assert.Equal("valid", result["BUILD_ID"]);
            Assert.Equal("valid", result["IMAGE_ID"]);
            Assert.Equal("Valid", result["PRETTY_NAME"]);
            Assert.False(result.ContainsKey("NAMES"));
            Assert.False(result.ContainsKey("USERNAME"));
            Assert.False(result.ContainsKey("USER_ID"));
            Assert.False(result.ContainsKey("ID_LIKE"));
            Assert.False(result.ContainsKey("VERSION_IDENTIFIER"));
            Assert.False(result.ContainsKey("BUILD_IDS"));
            Assert.False(result.ContainsKey("IMAGE_IDENTIFIER"));
            Assert.False(result.ContainsKey("PRETTY_NAMES"));
        }

        [SFFact]
        public void TestParseHandlesCarriageReturnLineFeed()
        {
            // arrange
            var contents = "NAME=\"Test\"\r\nID=test\r\n";

            // act
            var result = OsReleaseReader.ParseOsReleaseContents(contents);

            // assert
            Assert.Equal(2, result.Count);
            Assert.Equal("Test", result["NAME"]);
            Assert.Equal("test", result["ID"]);
        }
    }
}
