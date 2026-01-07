/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator.Okta;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.Okta
{
    [TestFixture]
    public class SamlResponseParserTest
    {
        private SamlResponseParser _parser;

        [SetUp]
        public void SetUp()
        {
            _parser = new SamlResponseParser();
        }

        [Test]
        public void TestExtractPostbackUrlFromValidSamlResponse()
        {
            // Arrange
            var samlHtml = @"<html>
<body>
<form method=""post"" action=""https://myaccount.snowflakecomputing.com/fed/login"">
<input type=""hidden"" name=""SAMLResponse"" value=""..."" />
</form>
</body>
</html>";

            // Act
            var result = _parser.ExtractPostbackUrl(samlHtml);

            // Assert
            Assert.AreEqual("https://myaccount.snowflakecomputing.com/fed/login", result.ToString());
            Assert.AreEqual("myaccount.snowflakecomputing.com", result.Host);
            Assert.AreEqual("https", result.Scheme);
        }

        [Test]
        public void TestExtractPostbackUrlWithHtmlEncodedUrl()
        {
            // Arrange
            var samlHtml = @"<html>
<body>
<form method=""post"" action=""https://myaccount.snowflakecomputing.com/fed/login?param1=value1&amp;param2=value2"">
<input type=""hidden"" name=""SAMLResponse"" value=""..."" />
</form>
</body>
</html>";

            // Act
            var result = _parser.ExtractPostbackUrl(samlHtml);

            // Assert
            Assert.AreEqual("https://myaccount.snowflakecomputing.com/fed/login?param1=value1&param2=value2", result.ToString());
            Assert.AreEqual("myaccount.snowflakecomputing.com", result.Host);
        }

        [Test]
        public void TestExtractPostbackUrlThrowsWhenFormNotFound()
        {
            // Arrange
            var samlHtml = @"<html>
<body>
<div>No form here</div>
</body>
</html>";

            // Act & Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _parser.ExtractPostbackUrl(samlHtml));
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_NOTFOUND.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }

        [Test]
        public void TestExtractPostbackUrlThrowsWhenActionNotFound()
        {
            // Arrange
            var samlHtml = @"<html>
<body>
<form method=""post"">
<input type=""hidden"" name=""SAMLResponse"" value=""..."" />
</form>
</body>
</html>";

            // Act & Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _parser.ExtractPostbackUrl(samlHtml));
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_NOTFOUND.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }

        [Test]
        public void TestExtractPostbackUrlThrowsWhenUrlMalformed()
        {
            // Arrange
            var samlHtml = @"<html>
<body>
<form method=""post"" action=""not-a-valid-url"">
<input type=""hidden"" name=""SAMLResponse"" value=""..."" />
</form>
</body>
</html>";

            // Act & Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _parser.ExtractPostbackUrl(samlHtml));
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_NOTFOUND.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }
    }
}
