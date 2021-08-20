/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    class SFStorageClientFactory
    {
        /// <summary>
        /// Instantiate a new storage client.
        /// </summary>
        /// <param name="stageInfo">The stage info used to create the client.</param>
        /// <returns>A new instance of the storage client.</returns>
        internal static ISFStorageClient newStorageClient(PutGetStageinfo stageInfo)
        {
            throw new NotImplementedException();
        }
    }
}
