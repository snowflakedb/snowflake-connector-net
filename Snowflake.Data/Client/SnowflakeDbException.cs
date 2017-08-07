/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Client
{
    public sealed class SnowflakeDbException : DbException
    {
        private string sqlState;

        private int vendorCode;

        private string errorMessage;

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

        public SnowflakeDbException(string sqlState, int vendorCode, string errorMessage)
        {
            this.sqlState = sqlState;
            this.vendorCode = vendorCode;
            this.errorMessage = errorMessage;
        }

        public override string ToString()
        {
            return string.Format("Error: {0} SqlState: {1}, VendorCode: {2}", errorMessage, sqlState, vendorCode);
        }
    }
}
