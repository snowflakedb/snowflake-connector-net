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

#pragma warning disable CS0108, CS0114 // TODO SNOW-3662960
        public string SqlState { get; private set; }
#pragma warning restore CS0108, CS0114
        private int VendorCode;

        public string QueryId { get; set; }

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
            SqlState = sqlState;
            VendorCode = vendorCode;
            QueryId = queryId;
        }

        public SnowflakeDbException(SFError error, string queryId, Exception innerException)
            : base(FormatExceptionMessage(error, [innerException.Message], string.Empty, queryId), innerException)
        {
            VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            QueryId = queryId;
        }

        public SnowflakeDbException(SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, string.Empty, string.Empty))
        {
            VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(string sqlState, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, sqlState, string.Empty))
        {
            VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            SqlState = sqlState;
        }

        public SnowflakeDbException(Exception innerException, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, string.Empty, string.Empty), innerException)
        {
            VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
        }

        public SnowflakeDbException(Exception innerException, string sqlState, SFError error, params object[] args)
            : base(FormatExceptionMessage(error, args, sqlState, string.Empty), innerException)
        {
            VendorCode = error.GetAttribute<SFErrorAttr>().errorCode;
            SqlState = sqlState;
        }

        private SnowflakeDbException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Creates a SnowflakeDbException with a specific query ID and an inner exception.
        /// Used to attach the server-side query identifier to timeout and cancellation errors
        /// so callers can correlate client exceptions with the Snowflake query history.
        /// </summary>
        /// <param name="innerException">The exception that caused this error.</param>
        /// <param name="error">The driver error descriptor.</param>
        /// <param name="queryId">The Snowflake query ID, or null if not yet available.</param>
        /// <param name="args">Format arguments for the error message template.</param>
        internal static SnowflakeDbException WithQueryId(Exception innerException, SFError error, string queryId, params object[] args)
        {
            var formatExceptionMessage = FormatExceptionMessage(error, args, string.Empty, queryId);
            var result = new SnowflakeDbException(formatExceptionMessage, innerException)
            {
                VendorCode = error.GetAttribute<SFErrorAttr>().errorCode,
                QueryId = queryId
            };

            return result;
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
