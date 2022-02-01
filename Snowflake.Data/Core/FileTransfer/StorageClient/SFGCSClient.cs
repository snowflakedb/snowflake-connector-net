/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Snowflake.Data.Log;

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
        private const string GCS_FILE_HEADER_DIGEST = "gcs-file-header-digest";
        private const string GCS_FILE_HEADER_CONTENT_LENGTH = "gcs-file-header-content-length";
        private const string GCS_FILE_HEADER_ENCRYPTION_METADATA = "gcs-file-header-encryption-metadata";

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
        /// The HTTP client to make requests.
        /// </summary>
        private static readonly HttpClient HttpClient = new HttpClient();

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
            if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString())
            {
                return new FileHeader();
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
                    if (err.Message.Contains("401") ||
                        err.Message.Contains("403") ||
                        err.Message.Contains("404"))
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
                    Task <HttpResponseMessage> response = HttpClient.GetAsync(fileMetadata.presignedUrl);
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
                    // If file doesn't exist, GET request fails
                    HttpRequestException err = (HttpRequestException)ex.InnerException;
                    fileMetadata.lastError = err;
                    if (err.Message.Contains("401"))
                    {
                        fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                    }
                    else if (err.Message.Contains("403") ||
                        err.Message.Contains("500") ||
                        err.Message.Contains("503"))
                    {
                        fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                    }
                    else if (err.Message.Contains("404"))
                    {
                        fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
                    }
                    else
                    {
                        fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
                    }
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
            HttpClient.DefaultRequestHeaders.Add("x-goog-meta-sfc-digest", fileMetadata.SHA256_DIGEST);
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
                HttpRequestException err = (HttpRequestException)ex.InnerException;
                fileMetadata.lastError = err;
                if (err.Message.Contains("400") && GCS_ACCESS_TOKEN != null)
                {
                    fileMetadata.resultStatus = ResultStatus.RENEW_PRESIGNED_URL.ToString();
                }
                else if (err.Message.Contains("401"))
                {
                    fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
                }
                else if (err.Message.Contains("403") ||
                    err.Message.Contains("500") ||
                    err.Message.Contains("503"))
                {
                    fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
                }
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }
    }
}
