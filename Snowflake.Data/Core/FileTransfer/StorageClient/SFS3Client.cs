using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;

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
        internal const string AMZ_IV = "x-amz-iv";
        internal const string AMZ_KEY = "x-amz-key";
        internal const string AMZ_MATDESC = "x-amz-matdesc";
        internal const string SFC_DIGEST = "sfc-digest";

        /// <summary>
        /// The status of the request.
        /// </summary>
        internal const string EXPIRED_TOKEN = "ExpiredToken";
        internal const string NO_SUCH_KEY = "NoSuchKey";

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
            int parallel,
            ProxyCredentials proxyCredentials)
        {
            Logger.Debug("Setting up a new AWS client ");

            // Get the key id and secret key from the response
            stageInfo.stageCredentials.TryGetValue(AWS_KEY_ID, out string awsAccessKeyId);
            stageInfo.stageCredentials.TryGetValue(AWS_SECRET_KEY, out string awsSecretAccessKey);

            AmazonS3Config clientConfig;

            if (proxyCredentials != null)
            {
                clientConfig = new AmazonS3Config()
                {
                    ProxyHost = proxyCredentials.ProxyHost,
                    ProxyPort = proxyCredentials.ProxyPort,
                    ProxyCredentials = new NetworkCredential(proxyCredentials.ProxyUser, proxyCredentials.ProxyPassword)
                };
            }
            else
            {
                clientConfig = new AmazonS3Config();
            }

            SetCommonClientConfig(
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

        internal SFS3Client(
            PutGetStageInfo stageInfo,
            int maxRetry,
            int parallel,
            ProxyCredentials proxyCredentials,
            AmazonS3Client amazonS3ClientMock) : this(stageInfo, maxRetry, parallel, proxyCredentials)
        {
            // Inject the mock S3Client
            S3Client = amazonS3ClientMock;
        }

        /// <summary>
        /// Extract the bucket name and path from the stage location.
        /// </summary>
        /// <param name="stageLocation">The command stage location.</param>
        /// <returns>The remote location of the S3 file.</returns>
        public RemoteLocation ExtractBucketNameAndPath(string stageLocation)
        {
            // Expand '~' and '~user' expressions
            //if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            //{
            //    stageLocation = Path.GetFullPath(stageLocation);
            //}

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
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;

            GetObjectRequest request = GetFileHeaderRequest(ref client, fileMetadata);

            try
            {
                // Issue the GET request
                var task = client.GetObjectAsync(request);
                task.ConfigureAwait(false);
                task.Wait();

                using (GetObjectResponse response = task.Result)
                {
                    return HandleFileHeaderResponse(ref fileMetadata, response);
                }
            }
            catch (Exception ex)
            {
                HandleFileHeaderErr(ex.InnerException, fileMetadata); // S3 places the AmazonS3Exception on the InnerException on non-async calls
                return null;
            }
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The file header of the S3 file.</returns>
        public async Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;

            GetObjectRequest request = GetFileHeaderRequest(ref client, fileMetadata);

            GetObjectResponse response = null;
            try
            {
                // Issue the GET request
                response = await client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                HandleFileHeaderErr(ex, fileMetadata); // S3 throws the AmazonS3Exception on async calls
                return null;
            }

            return HandleFileHeaderResponse(ref fileMetadata, response);
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="client">The Amazon S3 client.</param>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <returns>The file header request.</returns>
        private GetObjectRequest GetFileHeaderRequest(ref AmazonS3Client client, SFFileMetadata fileMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Create the S3 request object
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = location.bucket,
                Key = location.key + fileMetadata.RemoteFileName()
            };
            return request;
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="response">The Amazon S3 response.</param>
        /// <returns>The file header of the S3 file.</returns>
        internal FileHeader HandleFileHeaderResponse(ref SFFileMetadata fileMetadata, GetObjectResponse response)
        {
            // Update the result status of the file metadata
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata
            {
                iv = GetMetadataCaseInsensitive(response.Metadata, AMZ_IV),
                key = GetMetadataCaseInsensitive(response.Metadata, AMZ_KEY),
                matDesc = GetMetadataCaseInsensitive(response.Metadata, AMZ_MATDESC)
            };

            return new FileHeader
            {
                digest = GetMetadataCaseInsensitive(response.Metadata, SFC_DIGEST),
                contentLength = response.ContentLength,
                encryptionMetadata = encryptionMetadata
            };
        }

        private string GetMetadataCaseInsensitive(MetadataCollection metadataCollection, string metadataKey)
        {
            var value = metadataCollection[metadataKey];
            if (value != null)
                return value;
            if (string.IsNullOrEmpty(metadataKey))
                return null;
            var keysCaseInsensitive = metadataCollection.Keys
                .Where(key => $"x-amz-meta-{metadataKey}".Equals(key, StringComparison.OrdinalIgnoreCase));
            return keysCaseInsensitive.Any() ? metadataCollection[keysCaseInsensitive.First()] : null;
        }

        /// <summary>
        /// Set the client configuration common to both client with and without client-side
        /// encryption.
        /// </summary>
        /// <param name="clientConfig">The client config to update.</param>
        /// <param name="region">The region if any.</param>
        /// <param name="endpoint">The endpoint if any.</param>
        internal static void SetCommonClientConfig(
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
            if (!string.IsNullOrEmpty(endpoint))
            {
                var start = endpoint.IndexOf('[');
                var end = endpoint.IndexOf(']');
                if (start > -1 && end > -1 && end > start)
                {
                    endpoint = endpoint.Substring(start + 1, end - start - 1);
                }

                if (!endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                {
                    endpoint = "https://" + endpoint;
                }

                clientConfig.ServiceURL = endpoint;
            }
            // The region information used to determine the endpoint for the service.
            // RegionEndpoint and ServiceURL are mutually exclusive properties.
            // If both stageInfo.endPoint and stageInfo.region have a value, the endPoint takes precedence
            else if ((null != region) && (0 != region.Length))
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
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata)
        {
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;
            PutObjectRequest putObjectRequest = GetPutObjectRequest(ref client, fileMetadata, fileBytesStream, encryptionMetadata);

            try
            {
                // Issue the POST/PUT request
                var task = client.PutObjectAsync(putObjectRequest);
                task.ConfigureAwait(false);
                task.Wait();
            }
            catch (Exception ex)
            {
                HandleUploadFileErr(ex.InnerException, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the S3 location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;
            PutObjectRequest putObjectRequest = GetPutObjectRequest(ref client, fileMetadata, fileBytesStream, encryptionMetadata);

            try
            {
                // Issue the POST/PUT request
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                HandleUploadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the S3 location.
        /// </summary>
        /// <param name="client"> Amazon S3 client.</param>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        /// <returns>The Put Object request.</returns>
        private PutObjectRequest GetPutObjectRequest(ref AmazonS3Client client, SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Create S3 PUT request
            fileBytesStream.Position = 0;
            PutObjectRequest putObjectRequest = new PutObjectRequest
            {
                BucketName = location.bucket,
                Key = location.key + fileMetadata.destFileName,
                InputStream = fileBytesStream,
                ContentType = HTTP_HEADER_VALUE_OCTET_STREAM
            };

            if (stageInfo.isClientSideEncrypted)
            {
                // Populate the S3 Request Metadata
                putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_IV, encryptionMetadata.iv);
                putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_KEY, encryptionMetadata.key);
                putObjectRequest.Metadata.Add(AMZ_META_PREFIX + AMZ_MATDESC, encryptionMetadata.matDesc);
            }

            return putObjectRequest;
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency)
        {
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;
            GetObjectRequest getObjectRequest = GetGetObjectRequest(ref client, fileMetadata);

            try
            {
                // Issue the GET request
                var task = client.GetObjectAsync(getObjectRequest);
                task.ConfigureAwait(false);
                task.Wait();

                using (GetObjectResponse response = task.Result)
                {
                    // Write to file
                    using (var fileStream = FileOperations.Instance.Create(fullDstPath))
                    {
                        response.ResponseStream.CopyTo(fileStream);
                    }
                }
            }
            catch (Exception ex)
            {
                HandleDownloadFileErr(ex.InnerException, fileMetadata);
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public async Task DownloadFileAsync(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency, CancellationToken cancellationToken)
        {
            // Get the client
            SFS3Client SFS3Client = (SFS3Client)fileMetadata.client;
            AmazonS3Client client = SFS3Client.S3Client;
            GetObjectRequest getObjectRequest = GetGetObjectRequest(ref client, fileMetadata);

            try
            {
                // Issue the GET request
                using (GetObjectResponse response = await client.GetObjectAsync(getObjectRequest, cancellationToken).ConfigureAwait(false))

                // Write to file
                using (var fileStream = FileOperations.Instance.Create(fullDstPath))
                {
                    response.ResponseStream.CopyTo(fileStream);
                }
            }
            catch (Exception ex)
            {
                HandleDownloadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        private GetObjectRequest GetGetObjectRequest(ref AmazonS3Client client, SFFileMetadata fileMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Create S3 GET request
            return new GetObjectRequest
            {
                BucketName = location.bucket,
                Key = location.key + fileMetadata.srcFileName,
            };
        }

        /// <summary>
        /// Handle file header error.
        /// </summary>
        /// <param name="ex">Exception from file header.</param>
        /// <param name="fileMetadata">The file metadata.</param>
        private void HandleFileHeaderErr(Exception ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to get file header: " + ex.Message);

            switch (ex)
            {
                case AmazonS3Exception exAws:
                    if (exAws.ErrorCode == EXPIRED_TOKEN || exAws.ErrorCode == HttpStatusCode.BadRequest.ToString())
                    {
                        fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                    }
                    else if (exAws.ErrorCode == NO_SUCH_KEY)
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                    }
                    else
                    {
                        fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                    }

                    break;
                default:
                    fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                    break;
            }
        }

        /// <summary>
        /// Handle file upload error.
        /// </summary>
        /// <param name="ex">Exception from file header.</param>
        /// <param name="fileMetadata">The file metadata.</param>
        private void HandleUploadFileErr(Exception ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to upload file: " + ex.Message);

            switch (ex)
            {
                case AmazonS3Exception exAws:
                    if (exAws.ErrorCode == EXPIRED_TOKEN)
                    {
                        fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                    }
                    else
                    {
                        fileMetadata.lastError = exAws;
                        fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                    }
                    break;

                case Exception exOther:
                    fileMetadata.lastError = exOther;
                    fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                    break;
            }
        }

        /// <summary>
        /// Handle file download error.
        /// </summary>
        /// <param name="ex">Exception from file header.</param>
        /// <param name="fileMetadata">The file metadata.</param>
        private void HandleDownloadFileErr(Exception ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to download file: " + ex.Message);

            switch (ex)
            {
                case AmazonS3Exception exAws:
                    if (exAws.ErrorCode == EXPIRED_TOKEN)
                    {
                        fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                    }
                    else
                    {
                        fileMetadata.lastError = exAws;
                        fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                    }
                    break;

                case Exception exOther:
                    fileMetadata.lastError = exOther;
                    fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                    break;
            }
        }
    }
}
