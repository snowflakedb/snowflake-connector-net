/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    /// <summary>
    /// Validates URLs during Okta authentication flow.
    /// </summary>
    internal class OktaUrlValidator : IOktaUrlValidator
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OktaUrlValidator>();

        /// <inheritdoc/>
        public void ValidateTokenOrSsoUrl(Uri tokenOrSsoUrl, Uri oktaUrl)
        {
            if (tokenOrSsoUrl.Scheme != oktaUrl.Scheme || tokenOrSsoUrl.Host != oktaUrl.Host)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_SSO_TOKEN_URL_MISMATCH, tokenOrSsoUrl.ToString(), oktaUrl.ToString());
                s_logger.Error("Different urls", e);
                throw e;
            }
        }

        /// <inheritdoc/>
        public void ValidatePostbackUrl(Uri postbackUrl, string sessionHost, string sessionScheme)
        {
            if (postbackUrl.Host != sessionHost || postbackUrl.Scheme != sessionScheme)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_SAML_POSTBACK_INVALID,
                    postbackUrl.ToString(),
                    sessionScheme + ":\\\\" + sessionHost);
                s_logger.Error("Different urls", e);
                throw e;
            }
        }
    }
}
