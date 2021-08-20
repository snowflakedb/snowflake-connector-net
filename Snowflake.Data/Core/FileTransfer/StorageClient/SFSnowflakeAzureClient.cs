/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The azure client used to transfer files to the remote Azure storage.
    /// </summary>
    class SFSnowflakeAzureClient : ISFStorageClient
    {
        /// <summary>
        /// The attribute in the credential map containing the shared access signature token.
        /// </summary>
        private static readonly string AZURE_SAS_TOKEN = "AZURE_SAS_TOKEN";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFSnowflakeAzureClient>();

        /// <summary>
        /// The cloud blob client to use to upload and download data on Azure.
        /// </summary>
        private CloudBlobClient CloudBlobClient;

        public SFSnowflakeAzureClient(PutGetStageinfo stageInfo)
        {
            Logger.Debug("Setting up a new Azure client ");

            StorageCredentials storageCredentials;
            if (stageInfo.stageCredentials.TryGetValue(AZURE_SAS_TOKEN, out string sasToken))
            {
                storageCredentials = new StorageCredentials(sasToken);
            }
            else
            {
                // Anonymous
                storageCredentials = new StorageCredentials();
            }
                
            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount
                (storageCredentials, 
                stageInfo.storageAccount, 
                stageInfo.endPoint, 
                useHttps: true);

            // Create a CloudBlobClient object from the storage account.
            CloudBlobClient CloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
        }

        public void Dowload()
        {
            throw new NotImplementedException();
        }

        public void Upload()
        {
            //CloudBlobClient.DefaultRequestOptions.ParallelOperationThreadCount = xx;

            throw new NotImplementedException();
        }
    }
}
