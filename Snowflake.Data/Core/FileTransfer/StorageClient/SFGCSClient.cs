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
        /// The custom HTTP client.
        /// </summary>
        private HttpClient CustomHttpClient = null;

        private WebRequest CustomWebRequest = null;

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

        internal void SetCustomHttpClient(HttpClientHandler mockHttpClient)
        {
            CustomHttpClient = new HttpClient(mockHttpClient);
        }

        internal void SetCustomWebRequest(WebRequest mockWebRequest)
        {
            CustomWebRequest = mockWebRequest;
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
            WebRequest request;

            if (string.IsNullOrEmpty(fileMetadata.presignedUrl))
            {
                request = WebRequest.Create(generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName));
                request.Headers.Add("Authorization", $"Bearer {AccessToken}");
                request.Method = method;
            }
            else
            {
                request = WebRequest.Create(fileMetadata.presignedUrl);
            }

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

            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Head, url))
            //{
            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        var task = httpClient.SendAsync(requestMessage);
            //        task.Wait();
            //        using (HttpResponseMessage response = task.Result)
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                string digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST).First();
            //                long contentLength = (long)response.Content.Headers.ContentLength;

            //                fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

            //                return new FileHeader
            //                {
            //                    digest = digest,
            //                    contentLength = contentLength
            //                };
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleFileHeaderErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                // Issue a HEAD request
                WebRequest request = CustomWebRequest == null ? FormBaseRequest(fileMetadata, "HEAD") : CustomWebRequest;

                using (WebResponse response = request.GetResponse())
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
                // presignedUrls have a different error handling for file headers
                if (fileMetadata.presignedUrl != null)
                {
                    HttpWebResponse response = (HttpWebResponse)ex.Response;
                    if (response.StatusCode == HttpStatusCode.Unauthorized ||
                        response.StatusCode == HttpStatusCode.Forbidden ||
                        response.StatusCode == HttpStatusCode.NotFound)
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                        return new FileHeader();
                    }
                }
                else
                {
                    HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                    fileMetadata = HandleFileHeaderErr(statusCode, fileMetadata);
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

            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Head, url))
            //{
            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        using (HttpResponseMessage response = await httpClient.SendAsync(requestMessage).ConfigureAwait(false))
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                string digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST).First();
            //                long contentLength = (long)response.Content.Headers.ContentLength;

            //                fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

            //                return new FileHeader
            //                {
            //                    digest = digest,
            //                    contentLength = contentLength
            //                };
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleFileHeaderErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                // Issue a HEAD request
                WebRequest request = CustomWebRequest == null ? FormBaseRequest(fileMetadata, "HEAD") : CustomWebRequest;

                using (WebResponse response = await request.GetResponseAsync().ConfigureAwait(false))
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
                // presignedUrls have a different error handling for file headers
                if (fileMetadata.presignedUrl != null)
                {
                    HttpWebResponse response = (HttpWebResponse)ex.Response;
                    if (response.StatusCode == HttpStatusCode.Unauthorized ||
                        response.StatusCode == HttpStatusCode.Forbidden ||
                        response.StatusCode == HttpStatusCode.NotFound)
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                        return new FileHeader();
                    }
                }
                else
                {
                    HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                    fileMetadata = HandleFileHeaderErr(statusCode, fileMetadata);
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

            //string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Put, url))
            //{
            //    SetUpRequestMessageForUpload(requestMessage, fileMetadata, encryptionMetadata, encryptionData, fileBytes);

            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        var task = httpClient.SendAsync(requestMessage);
            //        task.Wait();

            //        using (HttpResponseMessage response = task.Result)
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                fileMetadata.destFileSize = fileMetadata.uploadSize;
            //                fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleUploadFileErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                WebRequest request = GetUploadFileRequest(fileMetadata, encryptionMetadata, encryptionData);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                using (WebResponse response = request.GetResponse())
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                fileMetadata = HandleUploadFileErr(statusCode, fileMetadata);
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

            //string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Put, url))
            //{
            //    SetUpRequestMessageForUpload(requestMessage, fileMetadata, encryptionMetadata, encryptionData, fileBytes);
            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        using (HttpResponseMessage response = await httpClient.SendAsync(requestMessage).ConfigureAwait(false))
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                fileMetadata.destFileSize = fileMetadata.uploadSize;
            //                fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleUploadFileErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                WebRequest request = GetUploadFileRequest(fileMetadata, encryptionMetadata, encryptionData);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                WebResponse webResponse = await request.GetResponseAsync().ConfigureAwait(false);

                using (WebResponse response = await request.GetResponseAsync().ConfigureAwait(false))
                {
                    fileMetadata.destFileSize = fileMetadata.uploadSize;
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                }
            }
            catch (WebException ex)
            {
                HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                fileMetadata = HandleUploadFileErr(statusCode, fileMetadata);
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

        private WebRequest GetUploadFileRequest(SFFileMetadata fileMetadata, SFEncryptionMetadata encryptionMetadata, String encryptionData)
        {
            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName) :
                fileMetadata.presignedUrl;

            // Issue the POST/PUT request
            WebRequest request = CustomWebRequest == null ? FormBaseRequest(fileMetadata, "PUT") : CustomWebRequest;

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
            string url = string.IsNullOrEmpty(fileMetadata.presignedUrl) ?
                generateFileURL(fileMetadata.stageInfo.location, fileMetadata.srcFileName) :
                fileMetadata.presignedUrl;

            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Get, url))
            //{
            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        var task = httpClient.SendAsync(requestMessage);
            //        task.Wait();
            //        using (HttpResponseMessage response = task.Result)
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                // Write to file
            //                using (var fileStream = File.Create(fullDstPath))
            //                {
            //                    using (var readTask = response.Content.ReadAsStreamAsync())
            //                    {
            //                        readTask.Wait();
            //                        Stream responseStream = readTask.Result;

            //                        responseStream.CopyTo(fileStream);
            //                        responseStream.Flush();
            //                    }
            //                }

            //                HandleDownloadResponse(response, fileMetadata);

            //                fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleDownloadFileErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                // Issue the GET request
                WebRequest request = CustomWebRequest == null ? FormBaseRequest(fileMetadata, "GET") : CustomWebRequest;

                using (WebResponse response = request.GetResponse())
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
                HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                fileMetadata = HandleDownloadFileErr(statusCode, fileMetadata);
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

            //using (var requestMessage = new HttpRequestMessage(HttpMethod.Get, url))
            //{
            //    using (HttpClient httpClient = CustomHttpClient == null ? new HttpClient() : CustomHttpClient)
            //    {
            //        httpClient.DefaultRequestHeaders.Authorization = (new AuthenticationHeaderValue("Bearer", AccessToken));

            //        using (HttpResponseMessage response = await httpClient.SendAsync(requestMessage).ConfigureAwait(false))
            //        {
            //            if (response.IsSuccessStatusCode)
            //            {
            //                // Write to file
            //                using (var fileStream = File.Create(fullDstPath))
            //                {
            //                    using (var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            //                    {
            //                        responseStream.CopyTo(fileStream);
            //                        responseStream.Flush();
            //                    }
            //                }

            //                HandleDownloadResponse(response, fileMetadata);

            //                fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
            //            }
            //            else
            //            {
            //                fileMetadata.lastError = new HttpRequestException($"Response status code does not indicate success: {(int)response.StatusCode} ({response.ReasonPhrase})");
            //                fileMetadata = HandleDownloadFileErr(response.StatusCode, fileMetadata);
            //            }
            //        }
            //    }
            //}

            try
            {
                // Issue the GET request
                WebRequest request = CustomWebRequest == null ? FormBaseRequest(fileMetadata, "GET") : CustomWebRequest;

                using (WebResponse response = await request.GetResponseAsync().ConfigureAwait(false))
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
                HttpStatusCode statusCode = CustomWebRequest == null ? ((HttpWebResponse)ex.Response).StatusCode : (HttpStatusCode)CustomWebRequest.Timeout;
                fileMetadata = HandleDownloadFileErr(statusCode, fileMetadata);
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
        /// Handle download http response.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        private void HandleDownloadResponse(WebResponse response, SFFileMetadata fileMetadata)
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
