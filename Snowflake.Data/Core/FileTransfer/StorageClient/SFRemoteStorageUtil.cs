/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;

namespace Snowflake.Data.Core.FileTransfer
{
    /// <summary>
    /// The class containing file header information. 
    /// </summary>
    internal class FileHeader
    {
        public string digest { get; set; }
        public long contentLength { get; set; }
        public SFEncryptionMetadata encryptionMetadata { get; set; }
    }

    /// <summary>
    /// The interface for the storage clients. 
    /// </summary>
    class SFRemoteStorageUtil
    {
        /// <summary>
        /// Strings to indicate specific storage type. 
        /// </summary>
        public const string S3_FS = "S3";
        public const string AZURE_FS = "AZURE";
        public const string GCS_FS = "GCS";
        public const string LOCAL_FS = "LOCAL_FS";

        /// <summary>
        /// Amount of concurrency to use by default. 
        /// </summary>
        const int DEFAULT_CONCURRENCY = 1;

        /// <summary>
        /// Maximum amount of times to retry. 
        /// </summary>
        const int DEFAULT_MAX_RETRY = 5;

        /// <summary>
        /// Instantiate a new storage client.
        /// </summary>
        /// <param name="stageInfo">The stage info used to create the client.</param>
        /// <returns>A new instance of the storage client.</returns>
        internal static ISFRemoteStorageClient GetRemoteStorage(PutGetResponseData response, ProxyCredentials proxyCredentials = null)
        {
            PutGetStageInfo stageInfo = response.stageInfo;
            string stageLocationType = stageInfo.locationType;

            // Create the storage type based on location type
            if (stageLocationType == LOCAL_FS)
            {
                throw new NotImplementedException();
            }
            else if (stageLocationType == S3_FS)
            {
                return new SFS3Client(stageInfo,
                    DEFAULT_MAX_RETRY,
                    response.parallel,
                    proxyCredentials
                    );
            }
            else if (stageLocationType == AZURE_FS)
            {
                return new SFSnowflakeAzureClient(stageInfo);
            }
            else if (stageLocationType == GCS_FS)
            {
                return new SFGCSClient(stageInfo);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Encrypt then upload one file.
        /// </summary>
        /// <summary>
        /// <param name="fileMetadata">The file metadata of the file to upload</param>
        internal static void UploadOneFile(SFFileMetadata fileMetadata)
        {
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();

            byte[] fileBytes;
            // If encryption enabled, encrypt the file to be uploaded
            if (fileMetadata.encryptionMaterial != null)
            {
                if (fileMetadata.memoryStream != null)
                {
                    fileBytes = EncryptionProvider.EncryptStream(
                       fileMetadata.memoryStream,
                       fileMetadata.encryptionMaterial,
                       encryptionMetadata);
                }
                else
                {
                    fileBytes = EncryptionProvider.EncryptFile(
                        fileMetadata.realSrcFilePath,
                        fileMetadata.encryptionMaterial,
                        encryptionMetadata);
                }
            }
            else
            {
                if (fileMetadata.memoryStream != null)
                {
                    fileBytes = fileMetadata.memoryStream.ToArray();
                }
                else
                {
                    fileBytes = File.ReadAllBytes(fileMetadata.realSrcFilePath);
                }
            }

            int maxConcurrency = fileMetadata.parallel;
            int maxRetry = DEFAULT_MAX_RETRY;
            Exception lastErr = null;

            // Attempt to upload and retry if fails
            for (int retry = 0; retry < maxRetry; retry++)
            {
                ISFRemoteStorageClient client = fileMetadata.client;

                if (!fileMetadata.overwrite)
                {
                    // Get the file metadata
                    FileHeader fileHeader = client.GetFileHeader(fileMetadata);
                    if (fileHeader != null &&
                        fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString())
                    {
                        // File already exists
                        fileMetadata.destFileSize = 0;
                        fileMetadata.resultStatus = ResultStatus.SKIPPED.ToString();
                        return;
                    }
                }

                if (fileMetadata.overwrite || fileMetadata.resultStatus == ResultStatus.NOT_FOUND_FILE.ToString())
                {
                    // Upload the file
                    client.UploadFile(fileMetadata, fileBytes, encryptionMetadata);
                }

                if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() ||
                    fileMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString() ||
                    fileMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                {
                    return;
                }
                else if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY_WITH_LOWER_CONCURRENCY.ToString())
                {
                    lastErr = fileMetadata.lastError;

                    maxConcurrency = fileMetadata.parallel - Convert.ToInt32(retry * fileMetadata.parallel / maxRetry);
                    maxConcurrency = Math.Max(DEFAULT_CONCURRENCY, maxConcurrency);
                    fileMetadata.lastMaxConcurrency = maxConcurrency;

                    // Failed to upload file, retrying
                    double sleepingTime = Math.Min(Math.Pow(2, retry), 16);
                    System.Threading.Thread.Sleep(Convert.ToInt32(sleepingTime));
                }
                else if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY.ToString())
                {
                    lastErr = fileMetadata.lastError;

                    // Failed to upload file, retrying
                    double sleepingTime = Math.Min(Math.Pow(2, retry), 16);
                    System.Threading.Thread.Sleep(Convert.ToInt32(sleepingTime));
                }
            }
            if (lastErr != null)
            {
                throw lastErr;
            }
            else
            {
                string msg = "Unknown Error in uploading a file: " + fileMetadata.destFileName;
                throw new Exception(msg);
            }
        }

        /// <summary>
        /// Attempt upload of a file and retry if fails.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to upload</param>
        internal static void UploadOneFileWithRetry(SFFileMetadata fileMetadata)
        {
            // Upload the file
            UploadOneFile(fileMetadata);
            if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString())
            {
                for (int count = 0; count < 10; count++)
                {
                    // Get the file metadata
                    fileMetadata.client.GetFileHeader(fileMetadata);
                    // Check result status if file already exists
                    if (fileMetadata.resultStatus == ResultStatus.NOT_FOUND_FILE.ToString())
                    {
                        // Wait 1 second
                        System.Threading.Thread.Sleep(1000);
                        continue;
                    }
                    break;
                }
            }
            return;
        }

        /// <summary>
        /// Download one file.
        /// </summary>
        /// <summary>
        /// <param name="fileMetadata">The file metadata of the file to download</param>
        internal static void DownloadOneFile(SFFileMetadata fileMetadata)
        {
            string fullDstPath = fileMetadata.localLocation;
            fullDstPath = Path.Combine(fullDstPath, fileMetadata.destFileName);

            // Check local location exists
            if (!Directory.Exists(fileMetadata.localLocation))
            {
                Directory.CreateDirectory(fileMetadata.localLocation);
            }

            ISFRemoteStorageClient client = fileMetadata.client;
            FileHeader fileHeader = client.GetFileHeader(fileMetadata);

            if (fileHeader != null)
            {
                fileMetadata.srcFileSize = fileHeader.contentLength;
            }

            int maxConcurrency = fileMetadata.parallel;
            Exception lastErr = null;
            int maxRetry = DEFAULT_MAX_RETRY;

            for (int retry = 0; retry < maxRetry; retry++)
            {
                // Download the file
                client.DownloadFile(fileMetadata, fullDstPath, maxConcurrency);

                if (fileMetadata.resultStatus == ResultStatus.DOWNLOADED.ToString())
                {
                    if (fileMetadata.encryptionMaterial != null)
                    {
                        /**
                          * For storage utils that do not have the privilege of
                          * getting the metadata early, both object and metadata
                          * are downloaded at once.In which case, the file meta will
                          * be updated with all the metadata that we need and
                          * then we can call getFileHeader to get just that and also
                          * preserve the idea of getting metadata in the first place.
                          * One example of this is the utils that use presigned url
                          * for upload / download and not the storage client library.
                          **/
                        if (fileMetadata.presignedUrl != null)
                        {
                            fileHeader = client.GetFileHeader(fileMetadata);
                        }

                        string tmpDstName = EncryptionProvider.DecryptFile(
                          fullDstPath,
                          fileMetadata.encryptionMaterial,
                          fileHeader.encryptionMetadata
                          );

                        File.Delete(fullDstPath);

                        // Copy decrypted tmp file to target destination path
                        File.Copy(tmpDstName, fullDstPath);

                        // Delete tmp file
                        File.Delete(tmpDstName);
                    }

                    FileInfo fileInfo = new FileInfo(fullDstPath);
                    fileMetadata.destFileSize = fileInfo.Length;
                    return;
                }
                else if (fileMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString() ||
                    fileMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                {
                    return;
                }
                else if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY_WITH_LOWER_CONCURRENCY.ToString())
                {
                    lastErr = fileMetadata.lastError;
                    // Failed to download file, retrying with max concurrency
                    maxConcurrency = fileMetadata.parallel - (retry * fileMetadata.parallel / maxRetry);
                    maxConcurrency = Math.Max(DEFAULT_CONCURRENCY, maxConcurrency);
                    fileMetadata.lastMaxConcurrency = maxConcurrency;

                    int sleepingTime = Convert.ToInt32(Math.Min(Math.Pow(2, retry), 16));
                    System.Threading.Thread.Sleep(sleepingTime);
                }
                else if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY.ToString())
                {
                    lastErr = fileMetadata.lastError;

                    int sleepingTime = Convert.ToInt32(Math.Min(Math.Pow(2, retry), 16));
                    System.Threading.Thread.Sleep(sleepingTime);
                }
            }
            if (lastErr != null)
            {
                throw lastErr;
            }
            else
            {
                var msg = "Unknown Error in downloading a file: " + fileMetadata.destFileName;
                throw lastErr;
            }
        }
    }
}
