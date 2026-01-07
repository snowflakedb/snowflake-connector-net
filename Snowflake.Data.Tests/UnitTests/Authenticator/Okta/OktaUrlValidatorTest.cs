/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator.Okta;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.Okta
{
    [TestFixture]
    public class OktaUrlValidatorTest
    {
        private OktaUrlValidator _validator;

        [SetUp]
        public void SetUp()
        {
            _validator = new OktaUrlValidator();
        }

        [Test]
        public void TestValidateTokenOrSsoUrlWithMatchingUrls()
        {
            // arrange
            var tokenOrSsoUrl = new Uri("https://mycompany.okta.com/app/snowflake/abc123");
            var oktaUrl = new Uri("https://mycompany.okta.com/");

            // act & assert - should not throw
            Assert.DoesNotThrow(() => _validator.ValidateTokenOrSsoUrl(tokenOrSsoUrl, oktaUrl));
        }

        [Test]
        public void TestValidateTokenOrSsoUrlThrowsOnSchemeMismatch()
        {
            // arrange
            var tokenOrSsoUrl = new Uri("http://mycompany.okta.com/app/snowflake/abc123");
            var oktaUrl = new Uri("https://mycompany.okta.com/");

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => _validator.ValidateTokenOrSsoUrl(tokenOrSsoUrl, oktaUrl));

            // assert
            Assert.AreEqual(SFError.IDP_SSO_TOKEN_URL_MISMATCH.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestValidateTokenOrSsoUrlThrowsOnHostMismatch()
        {
            // arrange
            var tokenOrSsoUrl = new Uri("https://attacker.okta.com/app/snowflake/abc123");
            var oktaUrl = new Uri("https://mycompany.okta.com/");

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => _validator.ValidateTokenOrSsoUrl(tokenOrSsoUrl, oktaUrl));

            // assert
            Assert.AreEqual(SFError.IDP_SSO_TOKEN_URL_MISMATCH.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestValidatePostbackUrlWithMatchingUrls()
        {
            // arrange
            var postbackUrl = new Uri("https://myaccount.snowflakecomputing.com/fed/login");
            var sessionHost = "myaccount.snowflakecomputing.com";
            var sessionScheme = "https";

            // act & assert - should not throw
            Assert.DoesNotThrow(() => _validator.ValidatePostbackUrl(postbackUrl, sessionHost, sessionScheme));
        }

        [Test]
        public void TestValidatePostbackUrlThrowsOnSchemeMismatch()
        {
            // arrange
            var postbackUrl = new Uri("http://myaccount.snowflakecomputing.com/fed/login");
            var sessionHost = "myaccount.snowflakecomputing.com";
            var sessionScheme = "https";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => _validator.ValidatePostbackUrl(postbackUrl, sessionHost, sessionScheme));

            // assert
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_INVALID.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestValidatePostbackUrlThrowsOnHostMismatch()
        {
            // arrange
            var postbackUrl = new Uri("https://attacker.snowflakecomputing.com/fed/login");
            var sessionHost = "myaccount.snowflakecomputing.com";
            var sessionScheme = "https";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => _validator.ValidatePostbackUrl(postbackUrl, sessionHost, sessionScheme));

            // assert
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_INVALID.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }
    }
}
