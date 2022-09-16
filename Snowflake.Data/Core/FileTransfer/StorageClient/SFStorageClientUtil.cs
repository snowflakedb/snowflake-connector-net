using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    class SFStorageClientUtil
    {
        /// <summary>
        /// The bad request error code.
        /// </summary>
        public static readonly string BAD_REQUEST_ERR = "400";

        /// <summary>
        /// The unauthorized error code.
        /// </summary>
        public static readonly string UNAUTHORIZED_ERR = "401";

        /// <summary>
        /// The forbidden error code.
        /// </summary>
        public static readonly string FORBIDDEN_ERR = "403";

        /// <summary>
        /// The not found error code.
        /// </summary>
        public static readonly string NOT_FOUND_ERR = "404";

        /// <summary>
        /// The internal server error code.
        /// </summary>
        public static readonly string INTERNAL_SERVER_ERR = "500";

        /// <summary>
        /// The server unavailable error code.
        /// </summary>
        public static readonly string SERVER_UNAVAILABLE_ERR = "503";
    }
}
