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

        public string QueryId { get; }

        public override int ErrorCode
        {
            get
            {
                return VendorCode;
            }
        }

        public SnowflakeDbException(string sqlState, int vendorCode, string errorMessage, string queryId)
            : base(FormatExceptionMessage(errorMessage, vendorCode, sqlState, queryId))
        {
            this.SqlState = sqlState;
            this.VendorCode = vendorCode;
            this.QueryId = queryId;
        }

        public SnowflakeDbException(SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, string.Empty, string.Empty))
        {
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(string sqlState, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, sqlState, string.Empty))
        {
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            this.SqlState = sqlState;
        }

        public SnowflakeDbException(Exception innerException, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, string.Empty, string.Empty), innerException)
        {
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(Exception innerException, string sqlState, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, sqlState, string.Empty), innerException)
        {
            this.VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            this.SqlState = sqlState;
        }

        static string FormatExceptionMessage(SFError error,
            object[] args,
            string sqlState,
            string queryId)
        {
            return FormatExceptionMessage(string.Format(rm.GetString(error.ToString()), args)
                , error.GetAttribute<SFErrorAttr>().errorCode
                , sqlState
                , queryId);
        }

        static string FormatExceptionMessage(string errorMessage,
            int vendorCode,
            string sqlState,
            string queryId)
        {
            return string.Format("Error: {0} SqlState: {1}, VendorCode: {2}, QueryId: {3}",
                errorMessage, sqlState, vendorCode, queryId);
        }
    }
}
