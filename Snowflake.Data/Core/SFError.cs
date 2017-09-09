/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Resources;

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
        DATA_READER_ALREADY_CLOSED
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
