/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The S3 client used to transfer files to the remote S3 storage.
    /// </summary>
    class SFS3Client : ISFRemoteStorageClient
    {
        /// <summary>
        /// The metadata of the S3 file.
        /// </summary>
        internal class S3Metadata
        {
            public string HTTP_HEADER_CONTENT_TYPE { get; set; }
            public string SFC_DIGEST { get; set; }
            public string AMZ_IV { get; set; }
            public string AMZ_KEY { get; set; }
            public string AMZ_MATDESC { get; set; }

        }

        /// <summary>
        /// The metadata header keys.
        /// </summary>
        private const string AMZ_META_PREFIX = "x-amz-meta-";
        private const string AMZ_IV = "x-amz-iv";
        private const string AMZ_KEY = "x-amz-key";
        private const string AMZ_MATDESC = "x-amz-matdesc";
        private const string SFC_DIGEST = "sfc-digest";

        /// <summary>
        /// The status of the request.
        /// </summary>
        private const string EXPIRED_TOKEN = "ExpiredToken";
        private const string NO_SUCH_KEY = "NoSuchKey";

        /// <summary>
        /// The application header type.
        /// </summary>
        private const string HTTP_HEADER_VALUE_OCTET_STREAM = "application/octet-stream";

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
            PutGetStageInfo stageInfo,
            int maxRetry,
            int parallel)
        {
            Logger.Debug("Setting up a new AWS client ");

            // Get the key id and secret key from the response
            stageInfo.stageCredentials.TryGetValue(AWS_KEY_ID, out string awsAccessKeyId);
            stageInfo.stageCredentials.TryGetValue(AWS_SECRET_KEY, out string awsSecretAccessKey);
            AmazonS3Config clientConfig = new AmazonS3Config();
            setCommonClientConfig(
                clientConfig,
                stageInfo.region,
                stageInfo.endPoint,
                maxRetry,
                parallel);
            
            // Get the AWS token value and create the S3 client
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
        /// Extract the bucket name and path from the stage location.
        /// </summary>
        /// <param name="stageLocation">The command stage location.</param>
        /// <returns>The remote location of the S3 file.</returns>
        public RemoteLocation ExtractBucketNameAndPath(string stageLocation)
        {
            // Expand '~' and '~user' expressions
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Console.WriteLine("stageLocation before: " + stageLocation);
                //stageLocation = Path.GetFullPath(stageLocation);
                Console.WriteLine("stageLocation after: " + Path.GetFullPath(stageLocation));
            }

            string bucketName = stageLocation;
            string s3path = "";

            // Split stage location as bucket name and path
            if (stageLocation.Contains("/"))
            {
                bucketName = stageLocation.Substring(0, stageLocation.IndexOf('/'));

                s3path = stageLocation.Substring(stageLocation.IndexOf('/') + 1, 
                    stageLocation.Length - stageLocation.IndexOf('/') - 1);
                if (s3path != null && !s3path.EndsWith("/"))
                {
                    s3path += '/';
                }
            }

            return new RemoteLocation()
            {
                bucket = bucketName,
                key = s3path
            };
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <returns>The file header of the S3 file.</returns>
        public FileHeader GetFileHeader(SFFileMetadata fileMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            Console.WriteLine("stage info location: " + stageInfo.location);

            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the client
            SFS3Client SFS3Client = (SFS3Client) fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;

            // Create the S3 request object
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = location.bucket,
                Key = location.key + fileMetadata.destFileName
            };

            Console.WriteLine("AWS bucket name: " + request.BucketName);
            Console.WriteLine("AWS key name: " + request.Key);

            GetObjectResponse response = null;
            try
            {
                // Issue the GET request
                var task = client.GetObjectAsync(request);
                task.Wait();

                response = task.Result;
            }
            catch (Exception ex)
            {
                Console.WriteLine("1 ex: " + ex.InnerException);

                AmazonS3Exception err = (AmazonS3Exception) ex.InnerException;
                if (err.ErrorCode == EXPIRED_TOKEN || err.ErrorCode == "400")
                {
                    fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                }
                else if (err.ErrorCode == NO_SUCH_KEY)
                {
                    fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                }
                else
                {
                    fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                }
                return null;
            }

            // Update the result status of the file metadata
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata{
                iv = response.Metadata[AMZ_IV],
                key = response.Metadata[AMZ_KEY],
                matDesc = response.Metadata[AMZ_MATDESC]
            };
            
            return new FileHeader
            {
                digest = response.Metadata[SFC_DIGEST],
                contentLength = response.ContentLength,
                encryptionMetadata = encryptionMetadata
            };
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

        /// <summary>
        /// Upload the file to the S3 location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;

            // Convert file bytes to memory stream
            Stream stream = new MemoryStream(fileBytes);

            // Create S3 PUT request
            PutObjectRequest putObjectRequest = new PutObjectRequest
            {
                BucketName = location.bucket,
                Key = location.key + fileMetadata.destFileName,
                InputStream = stream,
                ContentType = HTTP_HEADER_VALUE_OCTET_STREAM
            };

            // Populate the S3 Request Metadata
            putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_IV, encryptionMetadata.iv);
            putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_KEY, encryptionMetadata.key);
            putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_MATDESC, encryptionMetadata.matDesc);

            try
            {
                // Issue the POST/PUT request
                var task = client.PutObjectAsync(putObjectRequest);
                task.Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("2 ex: " + ex.InnerException);

                AmazonS3Exception err = (AmazonS3Exception)ex.InnerException;
                if (err.ErrorCode == EXPIRED_TOKEN)
                {
                    fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                }
                else
                {
                    fileMetadata.lastError = err;
                    fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                }
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }
    }
}