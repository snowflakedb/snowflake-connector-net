/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using System.Net;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The GCS client used to transfer files to the remote Google Cloud Storage.
    /// </summary>
    class SFGCSClient : ISFRemoteStorageClient
    {
        /// <summary>
        /// GCS header values.
        /// </summary>
        private const string GCS_METADATA_PREFIX = "x-goog-meta-";
        internal const string GCS_METADATA_SFC_DIGEST = GCS_METADATA_PREFIX + "sfc-digest";
        internal const string GCS_METADATA_MATDESC_KEY = GCS_METADATA_PREFIX + "matdesc";
        internal const string GCS_METADATA_ENCRYPTIONDATAPROP = GCS_METADATA_PREFIX + "encryptiondata";
        internal const string GCS_FILE_HEADER_CONTENT_LENGTH = "x-goog-stored-content-length";

        /// <summary>
        /// The GCS access token.
        /// </summary>
        private string AccessToken;

        /// <summary>
        /// The attribute in the credential map containing the access token.
        /// </summary>
        private static readonly string GCS_ACCESS_TOKEN = "GCS_ACCESS_TOKEN";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFGCSClient>();

        /// <summary>
        /// The storage client.
        /// </summary>
        private Google.Cloud.Storage.V1.StorageClient StorageClient;

        /// <summary>
        /// The custom WebRequest.
        /// </summary>
        private WebRequest _customWebRequest = null;

        /// <summary>
        /// GCS client with access token.
        /// </summary>
        /// <param name="stageInfo">The command stage info.</param>
        public SFGCSClient(PutGetStageInfo stageInfo)
        {
            Logger.Debug("Setting up a new GCS client ");

            if (stageInfo.stageCredentials.TryGetValue(GCS_ACCESS_TOKEN, out string accessToken))
            {
                Logger.Debug("Constructing client using access token");
                AccessToken = accessToken;
                GoogleCredential creds = GoogleCredential.FromAccessToken(accessToken, null);
                StorageClient = Google.Cloud.Storage.V1.StorageClient.Create(creds);
            }
            else
            {
                Logger.Info("No access token received from GS, constructing anonymous client with no encryption support");
                StorageClient = Google.Cloud.Storage.V1.StorageClient.CreateUnauthenticated();
            }
        }

        internal void SetCustomWebRequest(WebRequest mockWebRequest)
        {
            _customWebRequest = mockWebRequest;
        }

        /// <summary>
        /// Extract the bucket name and path from the stage location.
        /// </summary>
        /// <param name="stageLocation">The command stage location.</param>
        /// <returns>The remote location of the GCS file.</returns>
        public RemoteLocation ExtractBucketNameAndPath(string stageLocation)
        {
            string containerName = stageLocation;
            string gcsPath = "";

            // Split stage location as bucket name and path
            if (stageLocation.Contains("/"))
            {
                containerName = stageLocation.Substring(0, stageLocation.IndexOf('/'));

                gcsPath = stageLocation.Substring(stageLocation.IndexOf('/') + 1,
                    stageLocation.Length - stageLocation.IndexOf('/') - 1);
                if (gcsPath != null && !gcsPath.EndsWith("/"))
                {
                    gcsPath += '/';
                }
            }

            return new RemoteLocation()
            {
                bucket = containerName,
                key = gcsPath
            };
        }

        internal WebRequest FormBaseRequest(SFFileMetadata fileMetadata, string method)
        {
            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.RemoteFileName()) :
                fileMetadata.presignedUrl;

            WebRequest request = WebRequest.Create(url);
            request.Headers.Add("Authorization", $"Bearer {AccessToken}");
            request.Method = method;

            return request;
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>The file header of the GCS file.</returns>
        public FileHeader GetFileHeader(SFFileMetadata fileMetadata)
        {
            // If file already exists, return
            if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() ||
                fileMetadata.resultStatus == ResultStatus.DOWNLOADED.ToString())
            {
                return new FileHeader{
                    digest = fileMetadata.sha256Digest,
                    contentLength = fileMetadata.srcFileSize,
                    encryptionMetadata = fileMetadata.encryptionMetadata
                };
            }

            try
            {
                // Issue a HEAD request
                WebRequest request = _customWebRequest == null ? FormBaseRequest(fileMetadata, "HEAD") : _customWebRequest;

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    var digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST);
                    var contentLength = response.Headers.GetValues("content-length");

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest[0],
                        contentLength = Convert.ToInt64(contentLength[0])
                    };
                }
            }
            catch (WebException ex)
            {
                // GCS presigned urls have a different error handling for file headers
                fileMetadata = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                       HandleFileHeaderErrForGeneratedUrls(ex, fileMetadata) :
                       HandleFileHeaderErrForPresignedUrls(ex, fileMetadata);
            }

            return null;
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>The file header of the GCS file.</returns>
        public async Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            // If file already exists, return
            if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() ||
                fileMetadata.resultStatus == ResultStatus.DOWNLOADED.ToString())
            {
                return new FileHeader
                {
                    digest = fileMetadata.sha256Digest,
                    contentLength = fileMetadata.srcFileSize,
                    encryptionMetadata = fileMetadata.encryptionMetadata
                };
            }

            try
            {
                // Issue a HEAD request
                WebRequest request = _customWebRequest == null ? FormBaseRequest(fileMetadata, "HEAD") : _customWebRequest;

                using (HttpWebResponse response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
                {
                    var digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST);
                    var contentLength = response.Headers.GetValues("content-length");

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest[0],
                        contentLength = Convert.ToInt64(contentLength[0])
                    };
                }
            }
            catch (WebException ex)
            {
                // GCS presigned urls have a different error handling for file headers
                fileMetadata = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                       HandleFileHeaderErrForGeneratedUrls(ex, fileMetadata) :
                       HandleFileHeaderErrForPresignedUrls(ex, fileMetadata);
            }

            return null;
        }

        /// <summary>
        /// Generate the file URL.
        /// </summary>
        /// <param name="stageLocation">The GCS file metadata.</param>
        /// <param name="fileName">The GCS file metadata.</param>
        internal string generateFileURL(string stageLocation, string fileName)
        {
            var gcsLocation = ExtractBucketNameAndPath(stageLocation);
            var fullFilePath = gcsLocation.key + fileName;
            var link = "https://storage.googleapis.com/" + gcsLocation.bucket + "/" + fullFilePath;
            return link;
        }

        /// <summary>
        /// Upload the file to the GCS location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata)
        {
            String encryptionData = GetUploadEncryptionData(encryptionMetadata);

            try
            {
                WebRequest request = GetUploadFileRequest(fileMetadata, encryptionMetadata, encryptionData);

                Stream dataStream = request.GetRequestStream();
                fileBytesStream.Position = 0;
                fileBytesStream.CopyTo(dataStream);
                dataStream.Close();

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
            }
        }

        /// <summary>
        /// Upload the file to the GCS location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, Stream fileByteStream, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            String encryptionData = GetUploadEncryptionData(encryptionMetadata);

            try
            {
                WebRequest request = GetUploadFileRequest(fileMetadata, encryptionMetadata, encryptionData);

                Stream dataStream = await request.GetRequestStreamAsync().ConfigureAwait(false);
                fileByteStream.Position = 0;
                fileByteStream.CopyTo(dataStream);
                dataStream.Close();

                using (HttpWebResponse response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
            }
        }

        private WebRequest GetUploadFileRequest(SFFileMetadata fileMetadata, SFEncryptionMetadata encryptionMetadata, String encryptionData)
        {
            // Issue the POST/PUT request
            WebRequest request = _customWebRequest == null ? FormBaseRequest(fileMetadata, "PUT") : _customWebRequest;

            request.Headers.Add(GCS_METADATA_SFC_DIGEST, fileMetadata.sha256Digest);
            request.Headers.Add(GCS_METADATA_MATDESC_KEY, encryptionMetadata.matDesc);
            request.Headers.Add(GCS_METADATA_ENCRYPTIONDATAPROP, encryptionData);

            return request;
        }

        /// <summary>
        /// Get upload Encryption Data.
        /// </summary>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        /// <returns>Stream content.</returns>
        private String GetUploadEncryptionData(SFEncryptionMetadata encryptionMetadata)
        {
            // Create the encryption header value
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

            return encryptionData;
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency)
        {
            try
            {
                // Issue the GET request
                WebRequest request = _customWebRequest == null ? FormBaseRequest(fileMetadata, "GET") : _customWebRequest;                
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    // Write to file
                    using (var fileStream = File.Create(fullDstPath))
                    {
                        using (var responseStream = response.GetResponseStream())
                        {
                            responseStream.CopyTo(fileStream);
                            responseStream.Flush();
                        }
                    }
                    HandleDownloadResponse(response, fileMetadata);
                    fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
            }
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public async Task DownloadFileAsync(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency, CancellationToken cancellationToken)
        {
            try
            {
                // Issue the GET request
                WebRequest request = _customWebRequest == null ? FormBaseRequest(fileMetadata, "GET") : _customWebRequest;

                using (HttpWebResponse response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
                {
                    // Write to file
                    using (var fileStream = File.Create(fullDstPath))
                    {
                        using (var responseStream = response.GetResponseStream())
                        {
                            responseStream.CopyTo(fileStream);
                            responseStream.Flush();
                        }
                    }
                    HandleDownloadResponse(response, fileMetadata);
                    fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
            }
        }

        /// <summary>
        /// Handle download http response.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        private void HandleDownloadResponse(HttpWebResponse response, SFFileMetadata fileMetadata)
        {
            WebHeaderCollection headers = response.Headers;

            // Get header values
            dynamic encryptionData = JsonConvert.DeserializeObject(headers.Get(GCS_METADATA_ENCRYPTIONDATAPROP));
            string matDesc = headers.Get(GCS_METADATA_MATDESC_KEY);

            // Get encryption metadata from encryption data header value
            SFEncryptionMetadata encryptionMetadata = null;
            if (encryptionData != null)
            {
                encryptionMetadata = new SFEncryptionMetadata
                {
                    iv = encryptionData["ContentEncryptionIV"],
                    key = encryptionData["WrappedContentKey"]["EncryptedKey"],
                    matDesc = matDesc
                };
                fileMetadata.encryptionMetadata = encryptionMetadata;
            }

            fileMetadata.sha256Digest = headers.Get(GCS_METADATA_SFC_DIGEST);
            fileMetadata.srcFileSize = (long)Convert.ToDouble(headers.Get(GCS_FILE_HEADER_CONTENT_LENGTH));
        }

        /// <summary>
        /// Handle file header error for presigned urls.
        /// </summary>
        /// <param name="ex">The file header exception.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleFileHeaderErrForPresignedUrls(WebException ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to get file header for presigned url: " + ex.Message);
            
            HttpWebResponse response = (HttpWebResponse)ex.Response;
            if (response.StatusCode == HttpStatusCode.Unauthorized ||
                response.StatusCode == HttpStatusCode.Forbidden ||
                response.StatusCode == HttpStatusCode.NotFound)
            {
                fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
            }
            else
            {
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                fileMetadata.lastError = ex;
            }

            return fileMetadata;
        }

        /// <summary>
        /// Handle file header error for generated urls.
        /// </summary>
        /// <param name="ex">The file header exception.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleFileHeaderErrForGeneratedUrls(WebException ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to get file header for non-presigned url: " + ex.Message);

            HttpWebResponse response = (HttpWebResponse)ex.Response;
            if (response.StatusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                fileMetadata.lastError = ex;
            }
            else if (response.StatusCode == HttpStatusCode.Forbidden ||
                response.StatusCode == HttpStatusCode.InternalServerError ||
                response.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                fileMetadata.lastError = ex;
            }
            else if (response.StatusCode == HttpStatusCode.NotFound)
            {
                fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
            }
            else
            {
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                fileMetadata.lastError = ex;
            }
            return fileMetadata;
        }

        /// <summary>
        /// Handle file upload error.
        /// </summary>
        /// <param name="ex">The file header exception.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleUploadFileErr(WebException ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to upload file: " + ex.Message);

            fileMetadata.lastError = ex;

            HttpWebResponse response = (HttpWebResponse)ex.Response;
            if (response.StatusCode == HttpStatusCode.BadRequest && GCS_ACCESS_TOKEN != null)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_PRESIGNED_URL.ToString();
            }
            else if (response.StatusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (response.StatusCode == HttpStatusCode.Forbidden ||
                response.StatusCode == HttpStatusCode.InternalServerError ||
                response.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }

        /// <summary>
        /// Handle file download error.
        /// </summary>
        /// <param name="ex">The file header exception.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleDownloadFileErr(WebException ex, SFFileMetadata fileMetadata)
        {
            Logger.Error("Failed to download file: " + ex.Message);

            fileMetadata.lastError = ex;

            HttpWebResponse response = (HttpWebResponse)ex.Response;
            if (response.StatusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (response.StatusCode == HttpStatusCode.Forbidden ||
                response.StatusCode == HttpStatusCode.InternalServerError ||
                response.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }
    }
}
