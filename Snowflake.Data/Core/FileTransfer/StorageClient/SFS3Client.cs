/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Amazon;
using Amazon.Extensions.S3.Encryption;
using Amazon.Extensions.S3.Encryption.Primitives;
using Amazon.Internal;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The S3 client used to transfer files to the remote S3 storage.
    /// </summary>
    class SFS3Client
    {
        /// <summary>
        /// The attribute in the credential map containing the aws access key.
        /// </summary>
        private static readonly string AWS_KEY_ID = "AWS_KEY_ID";

        /// <summary>
        /// The attribute in the credential map containing the aws secret key id.
        /// </summary>
        private static readonly string AWS_SECRET_KEY = "AWS_SECRET_KEY";

        /// <summary>
        /// The attribute in the credential map containing the aws token.
        /// </summary>
        private static readonly string AWS_TOKEN = "AWS_TOKEN";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFS3Client>();

        /// <summary>
        /// The underlying S3 client.
        /// </summary>
        private AmazonS3Client S3Client;

        /// <summary>
        /// S3 client without client-side encryption.
        /// </summary>
        /// <param name="stageInfo">The command stage info.</param>
        public SFS3Client(
            PutGetStageinfo stageInfo, 
            bool isClientSideEncrypted, 
            int maxRetry, 
            int parallel)
        {
            Logger.Debug("Setting up a new Azure client ");

            stageInfo.stageCredentials.TryGetValue(AWS_KEY_ID, out string awsAccessKeyId);
            stageInfo.stageCredentials.TryGetValue(AWS_SECRET_KEY, out string awsSecretAccessKey);
            AmazonS3Config clientConfig = new AmazonS3Config();
            setCommonClientConfig(
                clientConfig,
                stageInfo.region,
                stageInfo.endPoint,
                maxRetry,
                parallel);

            if (stageInfo.stageCredentials.TryGetValue(AWS_TOKEN, out string awsSessionToken))
            {
                S3Client = new AmazonS3Client(
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    awsSessionToken,
                    clientConfig);
            }
            else
            {
                S3Client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, clientConfig);
            }

        }

        /// <summary>
        /// S3 client with client-side encryption.
        /// </summary>
        /// <param name="stageInfo">The command stage info.</param>
        /// <param name="encryptionMaterial">The encryption material.</param>
        public SFS3Client(
            PutGetStageinfo stageInfo, 
            PutGetEncryptionMaterial encryptionMaterial,
            int maxRetry,
            int parallel)
        {
            Logger.Debug("Setting up a new Azure client ");

            stageInfo.stageCredentials.TryGetValue(AWS_KEY_ID, out string awsAccessKeyId);
            stageInfo.stageCredentials.TryGetValue(AWS_SECRET_KEY, out string awsSecretAccessKey);

            Aes aesAlgorithm = Aes.Create();
            byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
            aesAlgorithm.Key = decodedMasterKey;
            EncryptionMaterialsV2 encryptionMaterials =
                new EncryptionMaterialsV2(aesAlgorithm, SymmetricAlgorithmType.AesGcm);
            AmazonS3CryptoConfigurationV2 clientConfig =
                new AmazonS3CryptoConfigurationV2(SecurityProfile.V2)
                {
                    StorageMode = CryptoStorageMode.ObjectMetadata
                };
            setCommonClientConfig(
                clientConfig, 
                stageInfo.region, 
                stageInfo.endPoint,
                maxRetry,
                parallel);

            if (stageInfo.stageCredentials.TryGetValue(AWS_TOKEN, out string awsSessionToken))
            {
                S3Client = new AmazonS3EncryptionClientV2(
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    awsSessionToken,
                    clientConfig,
                    encryptionMaterials);
            }
            else
            {
                S3Client = new AmazonS3EncryptionClientV2(
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    clientConfig,
                    encryptionMaterials);
            }
        }

        /// <summary>
        /// Set the client configuration common to both client with and without client-side 
        /// encryption.
        /// </summary>
        /// <param name="clientConfig">The client config to update.</param>
        /// <param name="region">The region if any.</param>
        /// <param name="endpoint">The endpoint if any.</param>
        private void setCommonClientConfig(
            AmazonS3Config clientConfig, 
            string region,
            string endpoint,
            int maxRetry,
            int parallel)
        {
            // Always return a regional URL
            clientConfig.USEast1RegionalEndpointValue = S3UsEast1RegionalEndpointValue.Regional;
            if ((null != region) && (0 != region.Length))
            {
                RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(region);
                clientConfig.RegionEndpoint = regionEndpoint;
            }

            // If a specific endpoint is specified use this
            if ((null != endpoint) && (0 != endpoint.Length))
            {
                clientConfig.ServiceURL = endpoint;
            }

            // The region information used to determine the endpoint for the service.
            // RegionEndpoint and ServiceURL are mutually exclusive properties. 
            // If both stageInfo.endPoint and stageInfo.region have a value, stageInfo.region takes
            // precedence and ServiceUrl will be reset to null.
            if ((null != region) && (0 != region.Length))
            {
                RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(region);
                clientConfig.RegionEndpoint = regionEndpoint;
            }

            // Unavailable for .net framework 4.6
            //clientConfig.MaxConnectionsPerServer = parallel;
            clientConfig.MaxErrorRetry = maxRetry;

        }
    }
}
