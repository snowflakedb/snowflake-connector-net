/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Resources;

namespace Snowflake.Data.Core
{
    /// <summary>
    ///     Exception thrown in the .net driver side.
    ///     Error Code from 270000 to 279999
    /// </summary>
    public class SFException : Exception
    {
        static private ResourceManager rm = new ResourceManager("Snowflake.Data.Core.ErrorMessages", 
            typeof(SFException).Assembly);

        private string errorMessage;

        private int errorCode;
        
        /// <summary>
        ///     Currently sqlState is only implemented for connection related issue. 
        /// </summary>
        private string sqlState;

        public override string Message { get { return errorMessage; } }

        public override IDictionary Data
        {
            get
            {
                return ErrorData;
            }
        }

        private IDictionary ErrorData = new Dictionary<string, object>();

        public SFException(SFError error, params object[] args)
        {
            errorMessage = String.Format(rm.GetString(error.ToString()), args);
            this.errorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            ErrorData["ErrorCode"] = this.errorCode;
        }

        public SFException(SFError error, string sqlState, params object[] args)
        {
            errorMessage = String.Format(rm.GetString(error.ToString()), args);
            this.errorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            this.sqlState = sqlState;
            ErrorData["ErrorCode"] = this.errorCode;
            ErrorData["SqlState"] = this.sqlState;
        }
    }
    class SFErrorAttr : Attribute
    {
        public int errorCode { get; set; }
    }

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
        UNSUPPORTED_FEATURE
    }
    
    public class SqlState
    {
        public const string WARNING = "01000";
    }

}
