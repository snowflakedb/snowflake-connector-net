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
        // Sql states not coming directly from the server.
        internal static string CONNECTION_FAILURE_SSTATE = "08006";

        static private ResourceManager rm = new ResourceManager("Snowflake.Data.Core.ErrorMessages",
            typeof(SnowflakeDbException).Assembly);

        public string SqlState { get; private set; }

        private int VendorCode;

        private string ErrorMessage;

        public string QueryId { get; }

        public override string Message
        {
            get
            {
                return ErrorMessage;
            }
        }

        public override int ErrorCode
        {
            get
            {
                return VendorCode;
            }
        }

        public SnowflakeDbException(string sqlState, int vendorCode, string errorMessage, string queryId)
        {
            this.SqlState = sqlState;
            this.VendorCode = vendorCode;
            this.ErrorMessage = errorMessage;
            this.QueryId = queryId;
        }

        public SnowflakeDbException(SFError error, params object[] args)
        {
            this.ErrorMessage = string.Format(rm.GetString(error.ToString()), args);
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(Exception innerException, SFError error, params object[] args)
            : base(string.Format(rm.GetString(error.ToString()), args), innerException)
        {
            this.ErrorMessage = string.Format(rm.GetString(error.ToString()), args);
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(Exception innerException, string sqlState, SFError error, params object[] args)
            : base(string.Format(rm.GetString(error.ToString()), args), innerException)
        {
            this.ErrorMessage = string.Format(rm.GetString(error.ToString()), args);
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            this.SqlState = sqlState;
        }

        public override string ToString()
        {
            return string.Format("Error: {0} SqlState: {1}, VendorCode: {2}, QueryId: {3}",
                ErrorMessage, SqlState, VendorCode, QueryId);
        }
    }
}
