/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using System.Linq;
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
        public const string GCS_METADATA_SFC_DIGEST = GCS_METADATA_PREFIX + "sfc-digest";
        public const string GCS_METADATA_MATDESC_KEY = GCS_METADATA_PREFIX + "matdesc";
        public const string GCS_METADATA_ENCRYPTIONDATAPROP = GCS_METADATA_PREFIX + "encryptiondata";
        public const string GCS_FILE_HEADER_CONTENT_LENGTH = "x-goog-stored-content-length";

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
        /// The HTTP client.
        /// </summary>
        private static HttpClient s_httpClient = new HttpClient();

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

            s_httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));
        }

        internal SFGCSClient(PutGetStageInfo stageInfo, DelegatingHandler mockHttpClient) : this(stageInfo)
        {
            s_httpClient = new HttpClient(mockHttpClient);
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

            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            using (var requestMessage = new HttpRequestMessage(HttpMethod.Head, url))
            {
                var task = s_httpClient.SendAsync(requestMessage);
                task.Wait();

                HttpResponseMessage response = task.Result;

                if (response.IsSuccessStatusCode)
                {
                    string digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST).First();
                    long contentLength = (long)response.Content.Headers.ContentLength;

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest,
                        contentLength = contentLength
                    };
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleFileHeaderErr(response.StatusCode, fileMetadata);
                }
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

            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            using (var requestMessage = new HttpRequestMessage(HttpMethod.Head, url))
            {
                HttpResponseMessage response = await s_httpClient.SendAsync(requestMessage).ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    string digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST).First();
                    long contentLength = (long)response.Content.Headers.ContentLength;

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest,
                        contentLength = contentLength
                    };
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleFileHeaderErr(response.StatusCode, fileMetadata);
                }
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
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata)
        {
            String encryptionData = GetUploadEncryptionData(encryptionMetadata);

            string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
            using (var requestMessage = new HttpRequestMessage(HttpMethod.Put, url))
            {
                SetUpRequestMessageForUpload(requestMessage, fileMetadata, encryptionMetadata, encryptionData, fileBytes);

                var task = s_httpClient.SendAsync(requestMessage);
                task.Wait();

                HttpResponseMessage response = task.Result;

                if (response.IsSuccessStatusCode)
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleUploadFileErr(response.StatusCode, fileMetadata);
                }
            }
        }

        /// <summary>
        /// Upload the file to the GCS location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            String encryptionData = GetUploadEncryptionData(encryptionMetadata);

            string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
            using (var requestMessage = new HttpRequestMessage(HttpMethod.Put, url))
            {
                SetUpRequestMessageForUpload(requestMessage, fileMetadata, encryptionMetadata, encryptionData, fileBytes);

                HttpResponseMessage response = await s_httpClient.SendAsync(requestMessage).ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleUploadFileErr(response.StatusCode, fileMetadata);
                }
            }
        }

        private void SetUpRequestMessageForUpload(HttpRequestMessage requestMessage,
            SFFileMetadata fileMetadata,
            SFEncryptionMetadata encryptionMetadata,
            String encryptionData,
            byte[] fileBytes)
        {
            requestMessage.Headers.Add(GCS_METADATA_SFC_DIGEST, fileMetadata.sha256Digest);
            requestMessage.Headers.Add(GCS_METADATA_MATDESC_KEY, encryptionMetadata.matDesc);
            requestMessage.Headers.Add(GCS_METADATA_ENCRYPTIONDATAPROP, encryptionData);

            ByteArrayContent byteContent = new ByteArrayContent(fileBytes);
            requestMessage.Content = byteContent;
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
            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            using (var requestMessage = new HttpRequestMessage(HttpMethod.Get, url))
            {
                var task = s_httpClient.SendAsync(requestMessage);
                task.Wait();
                HttpResponseMessage response = task.Result;

                if (response.IsSuccessStatusCode)
                {
                    // Write to file
                    using (var fileStream = File.Create(fullDstPath))
                    {
                        using (var readTask = response.Content.ReadAsStreamAsync())
                        {
                            readTask.Wait();
                            Stream responseStream = readTask.Result;

                            responseStream.CopyTo(fileStream);
                            responseStream.Flush();
                        }
                    }

                    HandleDownloadResponse(response, fileMetadata);

                    fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleDownloadFileErr(response.StatusCode, fileMetadata);
                }
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
            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            using (var requestMessage = new HttpRequestMessage(HttpMethod.Get, url))
            {
                HttpResponseMessage response = await s_httpClient.SendAsync(requestMessage).ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    // Write to file
                    using (var fileStream = File.Create(fullDstPath))
                    {
                        using (var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                        {
                            responseStream.CopyTo(fileStream);
                            responseStream.Flush();
                        }
                    }

                    HandleDownloadResponse(response, fileMetadata);

                    fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
                }
                else
                {
                    fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
                    fileMetadata = HandleDownloadFileErr(response.StatusCode, fileMetadata);
                }
            }
        }

        /// <summary>
        /// Handle download http response.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        private void HandleDownloadResponse(HttpResponseMessage response, SFFileMetadata fileMetadata)
        {
            HttpResponseHeaders headers = response.Headers;

            // Get header values
            dynamic encryptionData = JsonConvert.DeserializeObject(headers.GetValues(GCS_METADATA_ENCRYPTIONDATAPROP).First());
            string matDesc = headers.GetValues(GCS_METADATA_MATDESC_KEY).First();

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

            fileMetadata.sha256Digest = headers.GetValues(GCS_METADATA_SFC_DIGEST).First();
            fileMetadata.srcFileSize = (long)Convert.ToDouble(headers.GetValues(GCS_FILE_HEADER_CONTENT_LENGTH).First());
        }

        /// <summary>
        /// Handle file header error.
        /// </summary>
        /// <param name="statusCode">The HTTP status code.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleFileHeaderErr(HttpStatusCode statusCode, SFFileMetadata fileMetadata)
        {
            // If file doesn't exist, GET request fails
            if (statusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (statusCode == HttpStatusCode.Forbidden ||
                statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            else if (statusCode == HttpStatusCode.NotFound)
            {
                fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
            }
            else
            {
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
            }
            return fileMetadata;
        }

        /// <summary>
        /// Handle file upload error.
        /// </summary>
        /// <param name="statusCode">The HTTP status code.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleUploadFileErr(HttpStatusCode statusCode, SFFileMetadata fileMetadata)
        {
            if (statusCode == HttpStatusCode.BadRequest && GCS_ACCESS_TOKEN != null)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_PRESIGNED_URL.ToString();
            }
            else if (statusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (statusCode == HttpStatusCode.Forbidden ||
                statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }

        /// <summary>
        /// Handle file download error.
        /// </summary>
        /// <param name="statusCode">The HTTP status code.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <returns>File Metadata</returns>
        private SFFileMetadata HandleDownloadFileErr(HttpStatusCode statusCode, SFFileMetadata fileMetadata)
        {
            if (statusCode == HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (statusCode == HttpStatusCode.Forbidden ||
                statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }
    }
}
