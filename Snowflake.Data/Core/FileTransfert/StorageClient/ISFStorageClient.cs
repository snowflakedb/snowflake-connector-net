/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core.FileTransfert
{
    /// <summary>
    /// The interface for the storage clients. 
    /// </summary>
    interface ISFStorageClient
    {
        /// <summary>
        /// Upload data from a stream.
        /// </summary>
        void Upload();
        /// <summary>
        ///  Download data into a stream.
        ///  </summary>
        void Dowload();
    }
}
