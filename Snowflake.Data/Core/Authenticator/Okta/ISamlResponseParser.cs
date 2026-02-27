/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    /// <summary>
    /// Interface for parsing SAML HTML responses to extract the postback URL.
    /// </summary>
    internal interface ISamlResponseParser
    {
        /// <summary>
        /// Extracts the postback URL from the SAML HTML response.
        /// </summary>
        /// <param name="samlHtml">The HTML content of the SAML response containing a form with an action attribute.</param>
        /// <returns>The extracted postback URL as a Uri.</returns>
        /// <exception cref="Client.SnowflakeDbException">Thrown when the postback URL cannot be extracted from the HTML.</exception>
        Uri ExtractPostbackUrl(string samlHtml);
    }
}
