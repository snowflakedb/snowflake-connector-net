/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using System.Linq;
using Newtonsoft.Json.Linq;
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
        private const string GCS_METADATA_SFC_DIGEST = GCS_METADATA_PREFIX + "sfc-digest";
        private const string GCS_METADATA_MATDESC_KEY = GCS_METADATA_PREFIX + "matdesc";
        private const string GCS_METADATA_ENCRYPTIONDATAPROP = GCS_METADATA_PREFIX + "encryptiondata";
        private const string GCS_FILE_HEADER_CONTENT_LENGTH = "x-goog-stored-content-length";

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

        private string AccessToken;

        /// <summary>
        /// GCS client with access token.
        /// </summary>
        /// <param name="stageInfo">The command stage info.</param>
        public SFGCSClient(PutGetStageInfo stageInfo)
        {
            Logger.Debug("Setting up a new GCS client ");

            if (stageInfo.stageCredentials.TryGetValue(GCS_ACCESS_TOKEN, out string accessToken))
            {
                Console.WriteLine("accessToken: " + accessToken);

                Logger.Debug("Constructing client using access token");

                AccessToken = accessToken;
                GoogleCredential creds = GoogleCredential.FromAccessToken(accessToken, null);
                StorageClient = Google.Cloud.Storage.V1.StorageClient.Create(creds);
            }
            else
            {
                Console.WriteLine("No accessToken");

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
                    Console.WriteLine("1 fileMetadata.presignedUrl: " + fileMetadata.presignedUrl);

                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(fileMetadata.presignedUrl);
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                }
                catch (WebException ex)
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
            }
            else
            {
                // Generate the file URL based on GCS location
                string url = generateFileURL(fileMetadata.stageInfo.location, fileMetadata.destFileName);
                try
                {
                    Console.WriteLine("2 fileMetadata.presignedUrl: " + fileMetadata.presignedUrl);

                    // Issue a GET response
                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(fileMetadata.presignedUrl);
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                    Console.WriteLine("AccessToken: " + AccessToken);
                    request.Headers.Add("Authorization", $"Bearer ${AccessToken}");

                    var digest = response.Headers.GetValues(GCS_METADATA_SFC_DIGEST);
                    var contentLength = response.Headers.GetValues("content-length");

                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

                    return new FileHeader
                    {
                        digest = digest.ToString(),
                        contentLength = Convert.ToInt64(contentLength)
                    };
                }
                catch (WebException ex)
                {
                    // If file doesn't exist, GET request fails
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
                    else if (response.StatusCode == HttpStatusCode.NotFound)
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
            try
            {
                // Issue the POST/PUT request
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(fileMetadata.presignedUrl);
                request.Method = "PUT";

                request.Headers.Add("x-goog-meta-sfc-digest", fileMetadata.sha256Digest);
                request.Headers.Add("x-goog-meta-matdesc", encryptionMetadata.matDesc);
                request.Headers.Add("x-goog-meta-encryptiondata", encryptionData);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException ex)
            {
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
                Console.WriteLine("3 fileMetadata.presignedUrl: " + fileMetadata.presignedUrl);

                // Issue the GET request
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(fileMetadata.presignedUrl);

                Console.WriteLine("request: " + request);
                Console.WriteLine("request.Method: " + request.Method);
                Console.WriteLine("request.Host: " + request.Host);
                Console.WriteLine("request.GetResponse() " + request.GetResponse());

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                Console.WriteLine("response.StatusCode: " + response.StatusCode);
                Console.WriteLine("response.GetResponseStream(): " + response.GetResponseStream());

                // Write to file
                using (var fileStream = File.Create(fullDstPath))
                {
                    using (var responseStream = response.GetResponseStream())
                    {
                        responseStream.CopyTo(fileStream);
                        responseStream.Flush();
                    }
                }

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
            catch (WebException ex)
            {
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
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }
    }
}
