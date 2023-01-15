/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.IO;
using Newtonsoft.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Net;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{

    /// <summary>
    /// The azure client used to transfer files to the remote Azure storage.
    /// </summary>
    class SFSnowflakeAzureClient : ISFRemoteStorageClient
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
        /// The application header type.
        /// </summary>
        private const string HTTP_HEADER_VALUE_OCTET_STREAM = "application/octet-stream";

        /// <summary>
        /// Some common headers
        /// </summary>
        public const string HOST = "Host";
        public const string CONTENT_TYPE = "content-type";
        public const string CONTENT_LENGTH = "content-length";
        public const string X_MS_BLOB_TYPE = "x-ms-blob-type";
        public const string X_MS_META_ENCRYPTIONDATA = "x-ms-meta-encryptiondata";
        public const string X_MS_META_MATDESC = "x-ms-meta-matdesc";
        public const string X_MS_META_SFCDIGEST = "x-ms-meta-sfcdigest";

        /// <summary>
        /// Azure client without client-side encryption.
        /// </summary>
        public SFSnowflakeAzureClient()
        {
            Logger.Debug("Setting up a new Azure client ");
        }

        /// <summary>
        /// Extract the bucket name and path from the stage location.
        /// </summary>
        /// <param name="stageLocation">The command stage location.</param>
        /// <returns>The remote location of the Azure file.</returns>
        public RemoteLocation ExtractBucketNameAndPath(string stageLocation)
        {
            string blobName = stageLocation;
            string azurePath = null;

            // Split stage location as bucket name and path
            if (stageLocation.Contains("/"))
            {
                blobName = stageLocation.Substring(0, stageLocation.IndexOf('/'));

                azurePath = stageLocation.Substring(stageLocation.IndexOf('/') + 1,
                    stageLocation.Length - stageLocation.IndexOf('/') - 1);
                if (azurePath != "" && !azurePath.EndsWith("/"))
                {
                    azurePath += "/";
                }
            }

            return new RemoteLocation()
            {
                bucket = blobName,
                key = azurePath
            };
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <returns>The file header of the Azure file.</returns>
        public FileHeader GetFileHeader(SFFileMetadata fileMetadata)
        {
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "GET", uri.Host);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    // Update the result status of the file metadata
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                    return HandleFileHeaderResponse(response);
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileHeaderErr(response.StatusCode).ToString();

                return null;
            }
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <returns>The file header of the Azure file.</returns>
        public async Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "GET", uri.Host);

                using (HttpWebResponse response = (HttpWebResponse) await request.GetResponseAsync())
                {
                    // Update the result status of the file metadata
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                    return HandleFileHeaderResponse(response);
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileHeaderErr(response.StatusCode).ToString();

                return null;
            }
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="response">The Amazon S3 response.</param>
        /// <returns>The file header of the S3 file.</returns>
        private FileHeader HandleFileHeaderResponse(HttpWebResponse response)
        {
            dynamic encryptionData = JsonConvert.DeserializeObject(response.Headers["x-ms-meta-encryptiondata"]);
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata
            {
                iv = encryptionData["ContentEncryptionIV"],
                key = encryptionData.WrappedContentKey["EncryptedKey"],
                matDesc = response.Headers[X_MS_META_MATDESC]
            };

            return new FileHeader
            {
                digest = response.Headers[X_MS_META_SFCDIGEST],
                contentLength = response.ContentLength,
                encryptionMetadata = encryptionMetadata
            };
        }

        /// <summary>
        /// Upload the file to the Azure location.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata)
        {
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "PUT", uri.Host, fileBytes.Length);                
                BuildRequestHeaders(request, encryptionMetadata, fileMetadata.sha256Digest);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                request.GetResponse();
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleUploadFileErr(response.StatusCode).ToString();

                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the Azure location.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "PUT", uri.Host, fileBytes.Length);
                BuildRequestHeaders(request, encryptionMetadata, fileMetadata.sha256Digest);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                await request.GetResponseAsync();
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleUploadFileErr(response.StatusCode).ToString();

                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency)
        {
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "GET", uri.Host);

                // Write to file
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (var fileStream = File.Create(fullDstPath))
                using (var responseStream = response.GetResponseStream())
                {
                    responseStream.CopyTo(fileStream);
                    responseStream.Flush();
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleDownloadFileErr(response.StatusCode).ToString();

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
            Uri uri = GetFileUri(fileMetadata);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestBase(request, "GET", uri.Host);

                // Write to file
                using (HttpWebResponse response = (HttpWebResponse) await request.GetResponseAsync())
                using (var fileStream = File.Create(fullDstPath))
                using (var responseStream = response.GetResponseStream())
                {
                    responseStream.CopyTo(fileStream);
                    responseStream.Flush();
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleDownloadFileErr(response.StatusCode).ToString();

                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        /// <summary>
        /// Get the result status based on HTTP status code.
        /// </summary>
        /// <param name="statusCode">The HTTP error status code.</param>
        /// <returns>The file's result status.</returns>
        internal ResultStatus HandleFileHeaderErr(HttpStatusCode statusCode)
        {
            if (statusCode == HttpStatusCode.BadRequest)
            {
                return ResultStatus.RENEW_TOKEN;
            }
            else if (statusCode == HttpStatusCode.NotFound)
            {
                return ResultStatus.NOT_FOUND_FILE;
            }
            else
            {
                return ResultStatus.ERROR;
            }
        }

        /// <summary>
        /// Get the result status based on HTTP status code.
        /// </summary>
        /// <param name="statusCode">The HTTP error status code.</param>
        /// <returns>The file's result status.</returns>
        internal ResultStatus HandleUploadFileErr(HttpStatusCode statusCode)
        {
            if (statusCode == HttpStatusCode.BadRequest)
            {
                return ResultStatus.RENEW_PRESIGNED_URL;
            }
            else if (statusCode == HttpStatusCode.Unauthorized)
            {
                return ResultStatus.RENEW_TOKEN;
            }
            else if (statusCode == HttpStatusCode.Forbidden ||
                statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                return ResultStatus.NEED_RETRY;
            }
            else
            {
                return ResultStatus.ERROR;
            }
        }

        /// <summary>
        /// Get the result status based on HTTP status code.
        /// </summary>
        /// <param name="statusCode">The HTTP error status code.</param>
        /// <returns>The file's result status.</returns>
        internal ResultStatus HandleDownloadFileErr(HttpStatusCode statusCode)
        {
            if (statusCode == HttpStatusCode.Unauthorized)
            {
                return ResultStatus.RENEW_TOKEN;
            }
            else if (statusCode == HttpStatusCode.Forbidden ||
                statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                return ResultStatus.NEED_RETRY;
            }
            else
            {
                return ResultStatus.ERROR;
            }
        }

        /// <summary>
        /// Get the URI from the metadata stage info
        /// </summary>
        /// <param name="fileMetadata"></param>
        /// <returns></returns>
        private Uri GetFileUri(SFFileMetadata fileMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            string fileLocation = location.key + fileMetadata.destFileName;
            string host = stageInfo.storageAccount + "." + stageInfo.endPoint;

            string endpointUri = string.Format("https://{0}/{1}/{2}{3}",
                                   host,
                                   location.bucket,
                                   fileLocation,
                                   stageInfo.stageCredentials[AZURE_SAS_TOKEN]);

            return new Uri(endpointUri);
        }


        /// <summary>
        /// Build the headers for the request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="method"></param>
        /// <param name="host"></param>
        /// <param name="fileBytesLength"></param>
        private void BuildRequestBase(HttpWebRequest request, 
            string method, 
            string host, 
            int fileBytesLength = 0)
        {
            request.Method = method; 
            request.Host = host;
            request.ContentType = HTTP_HEADER_VALUE_OCTET_STREAM;

            if (fileBytesLength != 0)
            {
                request.ContentLength = fileBytesLength;
            }
        }

        /// <summary>
        /// Build the headers for the request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="encryptionMetadata"></param>
        /// <param name="sha256Digest"></param>
        private void BuildRequestHeaders(HttpWebRequest request,
            SFEncryptionMetadata encryptionMetadata, 
            string sha256Digest)
        {
            // Create the JSON for the encryption data header
            string encryptionData = JsonConvert.SerializeObject(new EncryptionData
            {
                EncryptionMode = "FullBlob",
                WrappedContentKey = new WrappedContentInfo
                {
                    KeyId = "symmKey1",
                    EncryptedKey = encryptionMetadata.key,
                    Algorithm = "AES_CBC_256"
                },
                EncryptionAgent = new EncryptionAgentInfo
                {
                    Protocol = "1.0",
                    EncryptionAlgorithm = "AES_CBC_256"
                },
                ContentEncryptionIV = encryptionMetadata.iv,
                KeyWrappingMetadata = new KeyWrappingMetadataInfo
                {
                    EncryptionLibrary = "Java 5.3.0"
                }
            });

            request.Headers.Add(X_MS_BLOB_TYPE, "BlockBlob");
            request.Headers.Add(X_MS_META_ENCRYPTIONDATA, encryptionData);
            request.Headers.Add(X_MS_META_MATDESC, encryptionMetadata.matDesc);
            request.Headers.Add(X_MS_META_SFCDIGEST, sha256Digest);
        }
    }
}
