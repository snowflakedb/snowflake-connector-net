/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using System.Linq;
using Newtonsoft.Json.Linq;

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
        private const string GCS_METADATA_SFC_DIGEST = GCS_METADATA_PREFIX + "sfc-digest";
        private const string GCS_METADATA_MATDESC_KEY = GCS_METADATA_PREFIX + "matdesc";
        private const string GCS_METADATA_ENCRYPTIONDATAPROP = GCS_METADATA_PREFIX + "encryptiondata";
        private const string GCS_FILE_HEADER_CONTENT_LENGTH = "x-goog-stored-content-length";

        /// <summary>
        /// The attribute in the credential map containing the access token.
        /// </summary>
        private static readonly string GCS_ACCESS_TOKEN = "GCS_ACCESS_TOKEN";

        /// <summary>
        /// The bad request error code.
        /// </summary>
        private static readonly string BAD_REQUEST_ERR = "400";

        /// <summary>
        /// The unauthorized error code.
        /// </summary>
        private static readonly string UNAUTHORIZED_ERR = "401";

        /// <summary>
        /// The forbidden error code.
        /// </summary>
        private static readonly string FORBIDDEN_ERR = "403";

        /// <summary>
        /// The not found error code.
        /// </summary>
        private static readonly string NOT_FOUND_ERR = "404";

        /// <summary>
        /// The internal server error code.
        /// </summary>
        private static readonly string INTERNAL_SERVER_ERR = "500";

        /// <summary>
        /// The server unavailable error code.
        /// </summary>
        private static readonly string SERVER_UNAVAILABLE_ERR = "503";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFGCSClient>();

        /// <summary>
        /// The storage client.
        /// </summary>
        private Google.Cloud.Storage.V1.StorageClient StorageClient;

        /// <summary>
        /// The HTTP client to make requests.
        /// </summary>
        private readonly HttpClient HttpClient;

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

                GoogleCredential creds = GoogleCredential.FromAccessToken(accessToken, null);
                StorageClient = Google.Cloud.Storage.V1.StorageClient.Create(creds);
            }
            else
            {
                Logger.Info("No access token received from GS, constructing anonymous client with no encryption support");
                StorageClient = Google.Cloud.Storage.V1.StorageClient.CreateUnauthenticated();
            }

            HttpClient = new HttpClient();
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

            if (fileMetadata.presignedUrl != null)
            {
                // Issue GET request to GCS file URL
                try
                {
                    Task<Stream> response = HttpClient.GetStreamAsync(fileMetadata.presignedUrl);
                    response.Wait();
                }
                catch (Exception ex)
                {
                    HttpRequestException err = (HttpRequestException)ex.InnerException;
                    if (err.Message.Contains(UNAUTHORIZED_ERR) ||
                        err.Message.Contains(FORBIDDEN_ERR) ||
                        err.Message.Contains(NOT_FOUND_ERR))
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                        return new FileHeader();
                    }
                }
            }
            else
            {
                // Generate the file URL based on GCS location
                string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
                try
                {
                    // Issue a GET response
                    HttpClient.DefaultRequestHeaders.Add("Authorization", "Bearer ${accessToken}");
                    Task<HttpResponseMessage> response = HttpClient.GetAsync(fileMetadata.presignedUrl);
                    response.Wait();

                    var digest = response.Result.Headers.GetValues(GCS_METADATA_SFC_DIGEST);
                    var contentLength = response.Result.Headers.GetValues("content-length");

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest.ToString(),
                        contentLength = Convert.ToInt64(contentLength)
                    };
                }
                catch (Exception ex)
                {
                    fileMetadata = HandleFileHeaderErr(ex, fileMetadata);
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

            if (fileMetadata.presignedUrl != null)
            {
                // Issue GET request to GCS file URL
                try
                {
                    Stream response = await HttpClient.GetStreamAsync(fileMetadata.presignedUrl).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    HttpRequestException err = (HttpRequestException)ex.InnerException;
                    if (err.Message.Contains(UNAUTHORIZED_ERR) ||
                        err.Message.Contains(FORBIDDEN_ERR) ||
                        err.Message.Contains(NOT_FOUND_ERR))
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                        return new FileHeader();
                    }
                }
            }
            else
            {
                // Generate the file URL based on GCS location
                string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
                try
                {
                    // Issue a GET response
                    HttpClient.DefaultRequestHeaders.Add("Authorization", "Bearer ${accessToken}");
                    HttpResponseMessage response = await HttpClient.GetAsync(fileMetadata.presignedUrl).ConfigureAwait(false);

                    var digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST);
                    var contentLength = response.Headers.GetValues("content-length");

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest.ToString(),
                        contentLength = Convert.ToInt64(contentLength)
                    };
                }
                catch (Exception ex)
                {
                    fileMetadata = HandleFileHeaderErr(ex, fileMetadata);
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

            // Set the meta header values
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-sfc-digest", fileMetadata.sha256Digest);
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-matdesc", encryptionMetadata.matDesc);
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-encryptiondata", encryptionData);

            // Convert file bytes to stream
            StreamContent strm = new StreamContent(new MemoryStream(fileBytes));
            // Set the stream content type
            strm.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            try
            {
                // Issue the POST/PUT request
                Task<HttpResponseMessage> response = HttpClient.PutAsync(fileMetadata.presignedUrl, strm);
                response.Wait();
            }
            catch (Exception ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the GCS location.
        /// </summary>
        /// <param name="fileMetadata">The GCS file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
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

            // Set the meta header values
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-sfc-digest", fileMetadata.sha256Digest);
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-matdesc", encryptionMetadata.matDesc);
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-encryptiondata", encryptionData);

            // Convert file bytes to stream
            StreamContent strm = new StreamContent(new MemoryStream(fileBytes));
            // Set the stream content type
            strm.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            try
            {
                // Issue the POST/PUT request
                HttpResponseMessage response = await HttpClient.PutAsync(fileMetadata.presignedUrl, strm, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
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
                // Issue the POST/PUT request
                var task = HttpClient.GetAsync(fileMetadata.presignedUrl);
                task.Wait();

                HttpResponseMessage response = task.Result;
                // Write to file
                using (var fileStream = File.Create(fullDstPath))
                {
                    var responseTask = response.Content.ReadAsStreamAsync();
                    responseTask.Wait();

                    responseTask.Result.CopyTo(fileStream);
                }

                HttpResponseHeaders headers = response.Headers;
                IEnumerable<string> values;

                // Get header values
                dynamic encryptionData = null;
                if (headers.TryGetValues(GCS_METADATA_ENCRYPTIONDATAPROP, out values))
                {
                    encryptionData = JsonConvert.DeserializeObject(values.First());
                }

                string matDesc = null;
                if (headers.TryGetValues(GCS_METADATA_MATDESC_KEY, out values))
                {
                    matDesc = values.First();
                }

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

                if (headers.TryGetValues(GCS_METADATA_SFC_DIGEST, out values))
                {
                    fileMetadata.sha256Digest = values.First();
                }

                if (headers.TryGetValues(GCS_FILE_HEADER_CONTENT_LENGTH, out values))
                {
                    fileMetadata.srcFileSize = (long)Convert.ToDouble(values.First());
                }
            }
            catch (Exception ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
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
                // Issue the POST/PUT request
                HttpResponseMessage response = await HttpClient.GetAsync(fileMetadata.presignedUrl).ConfigureAwait(false);
                // Write to file
                using (var fileStream = File.Create(fullDstPath))
                {
                    var stream = await response.Content.ReadAsStreamAsync();
                    stream.CopyTo(fileStream);
                }

                HttpResponseHeaders headers = response.Headers;
                IEnumerable<string> values;

                // Get header values
                dynamic encryptionData = null;
                if (headers.TryGetValues(GCS_METADATA_ENCRYPTIONDATAPROP, out values))
                {
                    encryptionData = JsonConvert.DeserializeObject(values.First());
                }

                string matDesc = null;
                if (headers.TryGetValues(GCS_METADATA_MATDESC_KEY, out values))
                {
                    matDesc = values.First();
                }

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

                if (headers.TryGetValues(GCS_METADATA_SFC_DIGEST, out values))
                {
                    fileMetadata.sha256Digest = values.First();
                }

                if (headers.TryGetValues(GCS_FILE_HEADER_CONTENT_LENGTH, out values))
                {
                    fileMetadata.srcFileSize = (long)Convert.ToDouble(values.First());
                }
            }
            catch (Exception ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        private SFFileMetadata HandleFileHeaderErr(Exception ex, SFFileMetadata fileMetadata)
        {
            // If file doesn't exist, GET request fails
            HttpRequestException err = (HttpRequestException)ex.InnerException;
            fileMetadata.lastError = err;
            if (err.Message.Contains(UNAUTHORIZED_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (err.Message.Contains(FORBIDDEN_ERR) ||
                err.Message.Contains(INTERNAL_SERVER_ERR) ||
                err.Message.Contains(SERVER_UNAVAILABLE_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            else if (err.Message.Contains(NOT_FOUND_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
            }
            else
            {
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
            }
            return fileMetadata;
        }

        private SFFileMetadata HandleUploadFileErr(Exception ex, SFFileMetadata fileMetadata)
        {
            HttpRequestException err = (HttpRequestException)ex.InnerException;
            fileMetadata.lastError = err;
            if (err.Message.Contains(BAD_REQUEST_ERR) && GCS_ACCESS_TOKEN != null)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_PRESIGNED_URL.ToString();
            }
            else if (err.Message.Contains(UNAUTHORIZED_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (err.Message.Contains(FORBIDDEN_ERR) ||
                err.Message.Contains(INTERNAL_SERVER_ERR) ||
                err.Message.Contains(SERVER_UNAVAILABLE_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }

        private SFFileMetadata HandleDownloadFileErr(Exception ex, SFFileMetadata fileMetadata)
        {
            HttpRequestException err = (HttpRequestException)ex.InnerException;
            fileMetadata.lastError = err;
            if (err.Message.Contains(UNAUTHORIZED_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (err.Message.Contains(FORBIDDEN_ERR) ||
                err.Message.Contains(INTERNAL_SERVER_ERR) ||
                err.Message.Contains(SERVER_UNAVAILABLE_ERR))
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }
    }
}
