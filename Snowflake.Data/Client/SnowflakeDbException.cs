/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using System.Resources;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    /// <summary>
    ///     Wraps the exception. 
    ///     If the exception is thrown in the client side, error code from 
    ///     270000 to 279999 will be used. Otherwise, server side error code
    ///     will be used. 
    /// </summary>
    public sealed class SnowflakeDbException : DbException
    {
        static private ResourceManager rm = new ResourceManager("Snowflake.Data.Core.ErrorMessages",
            typeof(SnowflakeDbException).Assembly);

        private string sqlState;

        private int vendorCode;

        private string errorMessage;

        public string queryId { get; }

        public override string Message
        {
            get
            {
                return errorMessage;
            }
        }

        public override int ErrorCode
        {
            get
            {
                return vendorCode;
            }
        }

        public SnowflakeDbException(string sqlState, int vendorCode, string errorMessage, string queryId)
        {
            this.sqlState = sqlState;
            this.vendorCode = vendorCode;
            this.errorMessage = errorMessage;
            this.queryId = queryId;
        }

        public SnowflakeDbException(SFError error, params object[] args)
        {
            this.errorMessage = string.Format(rm.GetString(error.ToString()), args);
            this.vendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(Exception innerException, SFError error, params object[] args)
            : base(string.Format(rm.GetString(error.ToString()), args), innerException)
        {
            this.errorMessage = string.Format(rm.GetString(error.ToString()), args);
            this.vendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public override string ToString()
        {
            return string.Format("Error: {0} SqlState: {1}, VendorCode: {2}, QueryId: {3}", 
                errorMessage, sqlState, vendorCode, queryId);
        }
    }
}
