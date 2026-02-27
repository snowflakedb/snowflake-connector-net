/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    /// <summary>
    /// Interface for validating URLs during Okta authentication flow.
    /// </summary>
    internal interface IOktaUrlValidator
    {
        /// <summary>
        /// Validates that a token or SSO URL matches the expected Okta URL.
        /// </summary>
        /// <param name="tokenOrSsoUrl">The token or SSO URL to validate.</param>
        /// <param name="oktaUrl">The expected Okta URL to validate against.</param>
        /// <exception cref="Client.SnowflakeDbException">
        /// Thrown with SFError.IDP_SSO_TOKEN_URL_MISMATCH if the scheme or host does not match.
        /// </exception>
        void ValidateTokenOrSsoUrl(Uri tokenOrSsoUrl, Uri oktaUrl);

        /// <summary>
        /// Validates that a SAML postback URL matches the expected session host and scheme.
        /// </summary>
        /// <param name="postbackUrl">The SAML postback URL to validate.</param>
        /// <param name="sessionHost">The expected session host.</param>
        /// <param name="sessionScheme">The expected session scheme (e.g., "https").</param>
        /// <exception cref="Client.SnowflakeDbException">
        /// Thrown with SFError.IDP_SAML_POSTBACK_INVALID if the scheme or host does not match.
        /// </exception>
        void ValidatePostbackUrl(Uri postbackUrl, string sessionHost, string sessionScheme);
    }
}
