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
        const string S3_FS = "S3";
        const string AZURE_FS = "AZURE";
        const string GCS_FS = "GCS";
        const string LOCAL_FS = "LOCAL_FS";

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
        internal static ISFRemoteStorageClient GetRemoteStorageType(PutGetResponseData response)
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
                    response.parallel
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
            byte[] fileBytes = File.ReadAllBytes(fileMetadata.realSrcFilePath);

            // If encryption enabled, encrypt the file to be uploaded
            if (fileMetadata.encryptionMaterial != null)
            {
                fileBytes = EncryptionProvider.CreateEncryptedBytes(
                    fileMetadata.realSrcFilePath,
                    fileMetadata.encryptionMaterial,
                    encryptionMetadata);
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
            bool breakFlag = false;

            for (int count = 0; count < 10; count++)
            {
                // Upload the file
                UploadOneFile(fileMetadata);
                if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString())
                {
                    for (int count2 = 0; count2 < 10; count2++)
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
                // Break out of loop if file is successfully uploaded or already exists
                if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() || 
                    fileMetadata.resultStatus == ResultStatus.SKIPPED.ToString())
                {
                    breakFlag = true;
                    break;
                }
            }
            if (!breakFlag)
            {
                // Could not upload a file even after retry
                fileMetadata.resultStatus = ResultStatus.ERROR.ToString();
            }
            return;
        }
    }
}
