using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;

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
        /// <param name="fileMetadata">The file metadata of the file to upload</param>
        internal static void UploadOneFile(SFFileMetadata fileMetadata)
        {
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            using (var fileBytesStreamPair = GetFileBytesStream(fileMetadata, encryptionMetadata))
            {

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
                        client.UploadFile(fileMetadata, fileBytesStreamPair.MainStream, encryptionMetadata);
                    }

                    if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() ||
                        fileMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString() ||
                        fileMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                    {
                        return;
                    }

                    HandleUploadResult(ref fileMetadata, ref maxConcurrency, ref lastErr, retry, maxRetry);
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
        }

        /// <summary>
        /// Encrypt then upload one file.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to upload</param>
        /// <param name="cancellationToken">The cancellation token</param>
        internal static async Task UploadOneFileAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            using (var fileBytesStreamPair = GetFileBytesStream(fileMetadata, encryptionMetadata))
            {

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
                        FileHeader fileHeader = await client.GetFileHeaderAsync(fileMetadata, cancellationToken)
                            .ConfigureAwait(false);
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
                        await client
                            .UploadFileAsync(fileMetadata, fileBytesStreamPair.MainStream, encryptionMetadata, cancellationToken)
                            .ConfigureAwait(false);
                    }

                    if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString() ||
                        fileMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString() ||
                        fileMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                    {
                        return;
                    }

                    HandleUploadResult(ref fileMetadata, ref maxConcurrency, ref lastErr, retry, maxRetry);
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
        }

        /// <summary>
        /// Handle upload result.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to upload.</param>
        /// <param name="maxConcurrency">The max Concurrency value.</param>
        /// <param name="lastErr">The Exception.</param>
        /// <param name="retry">The number of retry</param>
        /// <param name="maxRetry">The max retry</param>
        private static void HandleUploadResult(ref SFFileMetadata fileMetadata, ref int maxConcurrency, ref Exception lastErr, int retry, int maxRetry)
        {
            if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY_WITH_LOWER_CONCURRENCY.ToString())
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
        /// Attempt upload of a file and retry if fails.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to upload</param>
        internal static async Task UploadOneFileWithRetryAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            bool breakFlag = false;

            for (int count = 0; count < 10; count++)
            {
                // Upload the file
                await UploadOneFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                if (fileMetadata.resultStatus == ResultStatus.UPLOADED.ToString())
                {
                    for (int count2 = 0; count2 < 10; count2++)
                    {
                        // Get the file metadata
                        await fileMetadata.client.GetFileHeaderAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
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

        /// <summary>
        /// Download one file.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to download</param>
        internal static void DownloadOneFile(SFFileMetadata fileMetadata)
        {
            string fullDstPath = fileMetadata.localLocation;
            fullDstPath = Path.Combine(fullDstPath, fileMetadata.destFileName);

            // Check local location exists
            if (!Directory.Exists(fileMetadata.localLocation))
            {
                DirectoryOperations.Instance.CreateDirectory(fileMetadata.localLocation);
            }

            ISFRemoteStorageClient client = fileMetadata.client;

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
                        FileHeader fileHeader = null;
                        if (fileMetadata.presignedUrl != null)
                        {
                            fileHeader = client.GetFileHeader(fileMetadata);
                        }

                        SFEncryptionMetadata encryptionMetadata = fileHeader != null ? fileHeader.encryptionMetadata : fileMetadata.encryptionMetadata;

                        string tmpDstName = EncryptionProvider.DecryptFile(
                          fullDstPath,
                          fileMetadata.encryptionMaterial,
                          encryptionMetadata,
                          FileTransferConfiguration.FromFileMetadata(fileMetadata));

                        FileOperations.Instance.CopyFile(tmpDstName, fullDstPath);
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
                else
                {
                    HandleDownloadFileErr(ref fileMetadata, ref maxConcurrency, ref lastErr, retry, maxRetry);
                }
            }
            if (lastErr != null)
            {
                throw lastErr;
            }
            else
            {
                var msg = "Unknown Error in downloading a file: " + fileMetadata.destFileName;
                throw new Exception(msg);
            }
        }

        /// <summary>
        /// Download one file.
        /// </summary>
        /// <summary>
        /// <param name="fileMetadata">The file metadata of the file to download</param>
        internal static async Task DownloadOneFileAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            string fullDstPath = fileMetadata.localLocation;
            fullDstPath = Path.Combine(fullDstPath, fileMetadata.destFileName);

            // Check local location exists
            if (!Directory.Exists(fileMetadata.localLocation))
            {
                DirectoryOperations.Instance.CreateDirectory(fileMetadata.localLocation);
            }

            ISFRemoteStorageClient client = fileMetadata.client;

            int maxConcurrency = fileMetadata.parallel;
            Exception lastErr = null;
            int maxRetry = DEFAULT_MAX_RETRY;

            for (int retry = 0; retry < maxRetry; retry++)
            {
                // Download the file
                await client.DownloadFileAsync(fileMetadata, fullDstPath, maxConcurrency, cancellationToken).ConfigureAwait(false);

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
                        FileHeader fileHeader = null;
                        if (fileMetadata.presignedUrl != null)
                        {
                            fileHeader = await client.GetFileHeaderAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                        }

                        SFEncryptionMetadata encryptionMetadata = fileHeader != null ? fileHeader.encryptionMetadata : fileMetadata.encryptionMetadata;

                        string tmpDstName = EncryptionProvider.DecryptFile(
                          fullDstPath,
                          fileMetadata.encryptionMaterial,
                          encryptionMetadata,
                          FileTransferConfiguration.FromFileMetadata(fileMetadata));

                        FileOperations.Instance.CopyFile(tmpDstName, fullDstPath);
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
                else
                {
                    HandleDownloadFileErr(ref fileMetadata, ref maxConcurrency, ref lastErr, retry, maxRetry);
                }
            }
            if (lastErr != null)
            {
                throw lastErr;
            }
            else
            {
                var msg = "Unknown Error in downloading a file: " + fileMetadata.destFileName;
                throw new Exception(msg);
            }
        }

        /// <summary>
        /// Handle download result.
        /// </summary>
        /// <param name="fileMetadata">The file metadata of the file to download.</param>
        /// <param name="maxConcurrency">The max Concurrency value.</param>
        /// <param name="lastErr">The Exception.</param>
        /// <param name="retry">The number of retry</param>
        /// <param name="maxRetry">The max retry</param>
        private static void HandleDownloadFileErr(ref SFFileMetadata fileMetadata, ref int maxConcurrency, ref Exception lastErr, int retry, int maxRetry)
        {
            if (fileMetadata.resultStatus == ResultStatus.NEED_RETRY_WITH_LOWER_CONCURRENCY.ToString())
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

        private static StreamPair GetFileBytesStream(SFFileMetadata fileMetadata, SFEncryptionMetadata encryptionMetadata)
        {
            // If encryption enabled, encrypt the file to be uploaded
            if (fileMetadata.encryptionMaterial != null)
            {
                if (fileMetadata.memoryStream != null)
                {
                    return EncryptionProvider.EncryptStream(
                        fileMetadata.memoryStream,
                        fileMetadata.encryptionMaterial,
                        encryptionMetadata,
                        FileTransferConfiguration.FromFileMetadata(fileMetadata));
                }
                else
                {
                    return EncryptionProvider.EncryptFile(
                        fileMetadata.realSrcFilePath,
                        fileMetadata.encryptionMaterial,
                        encryptionMetadata,
                        FileTransferConfiguration.FromFileMetadata(fileMetadata));
                }
            }
            else
            {
                if (fileMetadata.memoryStream != null)
                {
                    return new StreamPair { MainStream = fileMetadata.memoryStream };
                }
                else
                {
                    return new StreamPair { MainStream = File.OpenRead(fileMetadata.realSrcFilePath) };
                }
            }
        }
    }
}
