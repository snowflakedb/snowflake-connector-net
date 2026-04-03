/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Web;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    /// <summary>
    /// Parses SAML HTML responses to extract the postback URL from the form action attribute.
    /// </summary>
    internal class SamlResponseParser : ISamlResponseParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SamlResponseParser>();

        /// <inheritdoc />
        public Uri ExtractPostbackUrl(string samlHtml)
        {
            try
            {
                int formIndex = samlHtml.IndexOf("<form");
                if (formIndex < 0)
                {
                    throw new InvalidOperationException("Could not find <form tag in SAML response");
                }

                int actionIndex = samlHtml.IndexOf("action=", formIndex);
                if (actionIndex < 0)
                {
                    throw new InvalidOperationException("Could not find action attribute in form");
                }

                // skip 'action="' (length = 8)
                int startIndex = actionIndex + 8;
                int endQuoteIndex = samlHtml.IndexOf('"', startIndex);
                if (endQuoteIndex < 0)
                {
                    throw new InvalidOperationException("Could not find closing quote for action attribute");
                }

                int length = endQuoteIndex - startIndex;
                string rawUrl = samlHtml.Substring(startIndex, length);
                string decodedUrl = HttpUtility.HtmlDecode(rawUrl);

                return new Uri(decodedUrl);
            }
            catch (Exception e)
            {
                s_logger.Error("Failed to extract SAML postback URL from HTML", e);
                throw new SnowflakeDbException(e, SFError.IDP_SAML_POSTBACK_NOTFOUND);
            }
        }
    }
}
