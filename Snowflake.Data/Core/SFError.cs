﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;

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

        [SFErrorAttr(errorCode = 270055)]
        INVALID_CONNECTION_PARAMETER_VALUE,

        [SFErrorAttr(errorCode = 270056)]
        INVALID_BROWSER_URL,

        [SFErrorAttr(errorCode = 270057)]
        BROWSER_RESPONSE_TIMEOUT,

        [SFErrorAttr(errorCode = 270058)]
        IO_ERROR_ON_GETPUT_COMMAND,

        [SFErrorAttr(errorCode = 270059)]
        EXECUTE_COMMAND_ON_CLOSED_CONNECTION,

        [SFErrorAttr(errorCode = 270060)]
        INCONSISTENT_RESULT_ERROR,

        [SFErrorAttr(errorCode = 270061)]
        STRUCTURED_TYPE_READ_ERROR,

        [SFErrorAttr(errorCode = 270062)]
        STRUCTURED_TYPE_READ_DETAILED_ERROR,

        [SFErrorAttr(errorCode = 390120)]
        EXT_AUTHN_DENIED,

        [SFErrorAttr(errorCode = 390123)]
        EXT_AUTHN_LOCKED,

        [SFErrorAttr(errorCode = 390126)]
        EXT_AUTHN_TIMEOUT,

        [SFErrorAttr(errorCode = 390127)]
        EXT_AUTHN_INVALID,

        [SFErrorAttr(errorCode = 390129)]
        EXT_AUTHN_EXCEPTION,
    }

    class SFMFATokenErrors
    {
        private static List<SFError> InvalidMFATokenErrors = new List<SFError>
        {
            SFError.EXT_AUTHN_DENIED,
            SFError.EXT_AUTHN_LOCKED,
            SFError.EXT_AUTHN_TIMEOUT,
            SFError.EXT_AUTHN_INVALID,
            SFError.EXT_AUTHN_EXCEPTION
        };

        public static bool IsInvalidMFATokenContinueError(int error)
        {
            return InvalidMFATokenErrors.Any(e => e.GetAttribute<SFErrorAttr>().errorCode == error);
        }
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
