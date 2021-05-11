/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core
{
    public enum SFError
    {
        [SFErrorAttr(errorCode = 270001)]
        INTERNAL_ERROR,

        [SFErrorAttr(errorCode = 270002)]
        COLUMN_INDEX_OUT_OF_BOUND,

        [SFErrorAttr(errorCode = 270003)]
        INVALID_DATA_CONVERSION,
        
        [SFErrorAttr(errorCode = 270004)]
        STATEMENT_ALREADY_RUNNING_QUERY,

        [SFErrorAttr(errorCode = 270005)]
        QUERY_CANCELLED,

        [SFErrorAttr(errorCode = 270006)]
        MISSING_CONNECTION_PROPERTY,

        [SFErrorAttr(errorCode = 270007)]
        REQUEST_TIMEOUT,

        [SFErrorAttr(errorCode = 270008)]
        INVALID_CONNECTION_STRING,

        [SFErrorAttr(errorCode = 270009)]
        UNSUPPORTED_FEATURE,

        [SFErrorAttr(errorCode = 270010)]
        DATA_READER_ALREADY_CLOSED,

        [SFErrorAttr(errorCode = 270011)]
        UNKNOWN_AUTHENTICATOR,

        [SFErrorAttr(errorCode = 270012)]
        UNSUPPORTED_PLATFORM,

        // Okta related
        [SFErrorAttr(errorCode = 270040)]
        IDP_SSO_TOKEN_URL_MISMATCH,

        [SFErrorAttr(errorCode = 270041)]
        IDP_SAML_POSTBACK_NOTFOUND,

        [SFErrorAttr(errorCode = 270042)]
        IDP_SAML_POSTBACK_INVALID,

        // External browser related
        [SFErrorAttr(errorCode = 270050)]
        BROWSER_RESPONSE_WRONG_METHOD,

        [SFErrorAttr(errorCode = 270051)]
        BROWSER_RESPONSE_INVALID_PREFIX,

        [SFErrorAttr(errorCode = 270052)]
        JWT_ERROR_READING_PK,

        [SFErrorAttr(errorCode = 270053)]
        UNSUPPORTED_DOTNET_TYPE,

        [SFErrorAttr(errorCode = 270054)]
        UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM,
    }

    class SFErrorAttr : Attribute
    {
        public int errorCode { get; set; }
    }
    
    public class SqlState
    {
        public const string WARNING = "01000";
    }

}
