/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Azure.Storage.Blobs;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.IO;
using Azure;
using Azure.Storage.Blobs.Models;
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
        /// The cloud blob client to use to upload and download data on Azure.
        /// </summary>
        private BlobServiceClient blobServiceClient;

        /// <summary>
        /// Azure client without client-side encryption.
        /// </summary>
        /// <param name="stageInfo">The command stage info.</param>
        public SFSnowflakeAzureClient(PutGetStageInfo stageInfo)
        {
            Logger.Debug("Setting up a new Azure client ");

            // Get the Azure SAS token and create the client
            if (stageInfo.stageCredentials.TryGetValue(AZURE_SAS_TOKEN, out string sasToken))
            {
                string blobEndpoint = string.Format("https://{0}.{1}", stageInfo.storageAccount, stageInfo.endPoint);
                blobServiceClient = new BlobServiceClient(new Uri(blobEndpoint),
                    new AzureSasCredential(sasToken));
            }
        }

        internal SFSnowflakeAzureClient(PutGetStageInfo stageInfo, BlobServiceClient blobServiceClientMock) : this(stageInfo)
        {
            // Inject the mock BlobServiceClient
            blobServiceClient = blobServiceClientMock;
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
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the Azure client
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(location.bucket);
            BlobClient blobClient = containerClient.GetBlobClient(location.key + fileMetadata.RemoteFileName());

            BlobProperties response;
            try
            {
                // Issue the GET request
                response = blobClient.GetProperties();
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleFileHeaderErr(ex, fileMetadata);
                return null;
            }

            return HandleFileHeaderResponse(ref fileMetadata, response);
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <returns>The file header of the Azure file.</returns>
        public async Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the Azure client
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(location.bucket);
            BlobClient blobClient = containerClient.GetBlobClient(location.key + fileMetadata.RemoteFileName());

            BlobProperties response;
            try
            {
                // Issue the GET request
                response = await blobClient.GetPropertiesAsync(null, cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleFileHeaderErr(ex, fileMetadata);
                return null;
            }

            return HandleFileHeaderResponse(ref fileMetadata, response);
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="response">The Amazon S3 response.</param>
        /// <returns>The file header of the S3 file.</returns>
        private FileHeader HandleFileHeaderResponse(ref SFFileMetadata fileMetadata, BlobProperties response)
        {
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();

            dynamic encryptionData = JsonConvert.DeserializeObject(response.Metadata["encryptiondata"]);
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata
            {
                iv = encryptionData["ContentEncryptionIV"],
                key = encryptionData.WrappedContentKey["EncryptedKey"],
                matDesc = response.Metadata["matdesc"]
            };

            return new FileHeader
            {
                digest = response.Metadata["sfcdigest"],
                contentLength = response.ContentLength,
                encryptionMetadata = encryptionMetadata
            };
        }

        /// <summary>
        /// Upload the file to the Azure location.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata)
        {
            // Create the metadata to use for the header
            IDictionary<string, string> metadata =
                new Dictionary<string, string>();

            BlobClient blobClient = GetUploadFileBlobClient(ref metadata, fileMetadata, encryptionMetadata);
            try
            {
                // Issue the POST/PUT request
                fileBytesStream.Position = 0;
                blobClient.Upload(fileBytesStream, overwrite: true);
                blobClient.SetMetadata(metadata);
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the Azure location.
        /// </summary>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <param name="fileBytesStream">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            // Create the metadata to use for the header
            IDictionary<string, string> metadata =
                new Dictionary<string, string>();

            BlobClient blobClient = GetUploadFileBlobClient(ref metadata, fileMetadata, encryptionMetadata);
            try
            {
                // Issue the POST/PUT request
                fileBytesStream.Position = 0;
                await blobClient.UploadAsync(fileBytesStream, true, cancellationToken).ConfigureAwait(false);
                blobClient.SetMetadata(metadata);
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleUploadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Get upload file blob client.
        /// </summary>
        /// <param name="metadata"> The header metadata</param>
        /// <param name="fileMetadata">The Azure file metadata.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        private BlobClient GetUploadFileBlobClient(ref IDictionary<string, string>metadata, SFFileMetadata fileMetadata, SFEncryptionMetadata encryptionMetadata)
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

            // Create the metadata to use for the header
            metadata.Add("encryptiondata", encryptionData);
            metadata.Add("matdesc", encryptionMetadata.matDesc);
            metadata.Add("sfcdigest", fileMetadata.sha256Digest);

            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the Azure client
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(location.bucket);
            return containerClient.GetBlobClient(location.key + fileMetadata.destFileName);
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the Azure client
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(location.bucket);
            BlobClient blobClient = containerClient.GetBlobClient(location.key + fileMetadata.srcFileName);

            try
            {
                // Issue the GET request
                blobClient.DownloadTo(fullDstPath);
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
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
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            // Get the Azure client
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(location.bucket);
            BlobClient blobClient = containerClient.GetBlobClient(location.key + fileMetadata.srcFileName);

            try
            {
                // Issue the GET request
                await blobClient.DownloadToAsync(fullDstPath, cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex)
            {
                fileMetadata = HandleDownloadFileErr(ex, fileMetadata);
                return;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        private SFFileMetadata HandleFileHeaderErr(RequestFailedException ex, SFFileMetadata fileMetadata)
        {
            if (ex.Status == (int)HttpStatusCode.BadRequest)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (ex.Status == (int)HttpStatusCode.NotFound)
            {
                fileMetadata.resultStatus = ResultStatus.NOT_FOUND_FILE.ToString();
            }
            else
            {
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
            }
            return fileMetadata;
        }

        private SFFileMetadata HandleUploadFileErr(RequestFailedException ex, SFFileMetadata fileMetadata)
        {
            if (ex.Status == (int)HttpStatusCode.BadRequest)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_PRESIGNED_URL.ToString();
            }
            else if (ex.Status == (int)HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (ex.Status == (int)HttpStatusCode.Forbidden ||
                ex.Status == (int)HttpStatusCode.InternalServerError ||
                ex.Status == (int)HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }

        private SFFileMetadata HandleDownloadFileErr(RequestFailedException ex, SFFileMetadata fileMetadata)
        {
            if (ex.Status == (int)HttpStatusCode.Unauthorized)
            {
                fileMetadata.resultStatus = ResultStatus.RENEW_TOKEN.ToString();
            }
            else if (ex.Status == (int)HttpStatusCode.Forbidden ||
                ex.Status == (int)HttpStatusCode.InternalServerError ||
                ex.Status == (int)HttpStatusCode.ServiceUnavailable)
            {
                fileMetadata.resultStatus = ResultStatus.NEED_RETRY.ToString();
            }
            return fileMetadata;
        }
    }
}
