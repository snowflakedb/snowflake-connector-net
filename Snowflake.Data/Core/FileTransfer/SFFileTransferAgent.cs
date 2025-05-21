using Snowflake.Data.Client;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// The status of the file to be uploaded/downloaded.
    /// </summary>
    enum ResultStatus
    {
        ERROR,
        UPLOADED,
        DOWNLOADED,
        COLLISION,
        SKIPPED,
        RENEW_TOKEN,
        RENEW_PRESIGNED_URL,
        NOT_FOUND_FILE,
        NEED_RETRY,
        NEED_RETRY_WITH_LOWER_CONCURRENCY
    }

    /// <summary>
    /// The command type of the query.
    /// </summary>
    internal enum CommandTypes
    {
        UPLOAD,
        DOWNLOAD
    }

    /// <summary>
    /// The type of the storage client.
    /// </summary>
    internal enum StorageClientType
    {
        LOCAL,
        REMOTE
    }

    /// <summary>
    /// Class responsible for uploading and downloading files to the remote client.
    /// </summary>
    class SFFileTransferAgent
    {
        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFFileTransferAgent>();

        /// <summary>
        /// Auto-detect keyword for source compression type auto detection.
        /// </summary>
        private static readonly string COMPRESSION_AUTO_DETECT = "auto_detect";

        /// <summary>
        /// The Snowflake query
        /// </summary>
        private string Query;

        /// <summary>
        /// The Snowflake session
        /// </summary>
        private SFSession Session;

        /// <summary>
        /// External cancellation token, used to stop the transfer
        /// </summary>
        private CancellationToken externalCancellationToken;

        /// <summary>
        /// The type of transfer either UPLOAD or DOWNLOAD.
        /// </summary>
        private readonly CommandTypes CommandType;

        /// <summary>
        /// The file metadata. Applies to all files being uploaded/downloaded
        /// </summary>
        private PutGetResponseData TransferMetadata;

        /// <summary>
        /// The path to the user home directory.
        /// </summary>
        private readonly string HomePath = (
            Environment.OSVersion.Platform == PlatformID.Unix ||
            Environment.OSVersion.Platform == PlatformID.MacOSX) ?
              Environment.GetEnvironmentVariable("HOME") :
              Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

        /// <summary>
        /// List of metadata for small and large files.
        /// </summary>
        private List<SFFileMetadata> FilesMetas = new List<SFFileMetadata>();
        private List<SFFileMetadata> SmallFilesMetas = new List<SFFileMetadata>();
        private List<SFFileMetadata> LargeFilesMetas = new List<SFFileMetadata>();

        /// <summary>
        /// List of metadata for the resulting file after upload/download.
        /// </summary>
        private List<SFFileMetadata> ResultsMetas = new List<SFFileMetadata>();

        /// <summary>
        /// List of encryption materials of the files to be uploaded/downloaded.
        /// </summary>
        private List<PutGetEncryptionMaterial> EncryptionMaterials = new List<PutGetEncryptionMaterial>();

        /// <summary>
        /// Time to wait before uploading the next file.
        /// </summary>
        private int INJECT_WAIT_IN_PUT = 0;

        /// <summary>
        /// String indicating local storage type.
        /// </summary>
        private const string LOCAL_FS = "LOCAL_FS";

        private const string STREAM_FILE_NAME = "stream";
        private MemoryStream memoryStream = null;
        private string streamDestFileName = null;
        private string destStagePath = null;

        /// <summary>
        /// Mutex for renewing expired client.
        /// </summary>
        private static Mutex RenewClientMutex = new Mutex();

        /// <summary>
        /// Placeholder threshold value using max long value.
        /// </summary>
        private long DATA_SIZE_THRESHOLD = Int64.MaxValue;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SFFileTransferAgent(
            string query,
            SFSession session,
            PutGetResponseData responseData,
            CancellationToken cancellationToken)
        {
            Query = query;
            Session = session;
            TransferMetadata = responseData;
            TransferMetadata.threshold = DATA_SIZE_THRESHOLD;
            CommandType = (CommandTypes)Enum.Parse(typeof(CommandTypes), TransferMetadata.command, true);
            externalCancellationToken = cancellationToken;
        }
        public SFFileTransferAgent(
            string query,
            SFSession session,
            PutGetResponseData responseData,
            ref MemoryStream inputStream,
            string filename,
            string stagePath,
            CancellationToken cancellationToken)
        {
            Query = query;
            Session = session;
            TransferMetadata = responseData;
            TransferMetadata.threshold = DATA_SIZE_THRESHOLD;
            memoryStream = inputStream;
            streamDestFileName = filename;
            destStagePath = stagePath;
            CommandType = CommandTypes.UPLOAD;
            externalCancellationToken = cancellationToken;
        }


        /// <summary>
        /// Execute the PUT/GET command.
        /// </summary>
        public void execute()
        {
            try
            {
                // Initialize the encryption metadata
                initEncryptionMaterial();

                if (CommandTypes.UPLOAD == CommandType)
                {
                    initFileMetadataForUpload();
                }
                else if (CommandTypes.DOWNLOAD == CommandType)
                {
                    initFileMetadata(TransferMetadata.src_locations);

                    Directory.CreateDirectory(TransferMetadata.localLocation);
                }

                // Update the file metadata with GCS presigned URL
                updatePresignedUrl();

                foreach (SFFileMetadata fileMetadata in FilesMetas)
                {
                    // If the file is larger than the threshold, add it to the large files list
                    // Otherwise add it to the small files list
                    if (fileMetadata.srcFileSize > TransferMetadata.threshold)
                    {
                        LargeFilesMetas.Add(fileMetadata);
                    }
                    else
                    {
                        SmallFilesMetas.Add(fileMetadata);
                    }
                }

                // Check command type
                if (CommandTypes.UPLOAD == CommandType)
                {
                    upload();
                }
                else if (CommandTypes.DOWNLOAD == CommandType)
                {
                    download();
                }
            }
            catch (Exception e)
            {
                Logger.Error("Error while transferring file(s): " + e.Message);
                if (e is SnowflakeDbException snowflakeException)
                {
                    if (snowflakeException.QueryId == null)
                    {
                        snowflakeException.QueryId = TransferMetadata.queryId;
                    }
                    throw snowflakeException;
                }
                throw new SnowflakeDbException(SFError.IO_ERROR_ON_GETPUT_COMMAND, TransferMetadata.queryId, e);
            }
        }

        public async Task executeAsync(CancellationToken cancellationToken)
        {
            // Initialize the encryption metadata
            initEncryptionMaterial();

            if (CommandTypes.UPLOAD == CommandType)
            {
                initFileMetadataForUpload();
            }
            else if (CommandTypes.DOWNLOAD == CommandType)
            {
                initFileMetadata(TransferMetadata.src_locations);

                Directory.CreateDirectory(TransferMetadata.localLocation);
            }

            // Update the file metadata with GCS presigned URL
            await updatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);

            foreach (SFFileMetadata fileMetadata in FilesMetas)
            {
                // If the file is larger than the threshold, add it to the large files list
                // Otherwise add it to the small files list
                if (fileMetadata.srcFileSize > TransferMetadata.threshold)
                {
                    LargeFilesMetas.Add(fileMetadata);
                }
                else
                {
                    SmallFilesMetas.Add(fileMetadata);
                }
            }

            // Check command type
            if (CommandTypes.UPLOAD == CommandType)
            {
                await uploadAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (CommandTypes.DOWNLOAD == CommandType)
            {
                await downloadAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generate the result set based on the file metadata.
        /// </summary>
        /// <returns>The result set containing file status and info</returns>
        public SFResultSet result()
        {
            // Set the row count using the number of metadata in the result metas
            TransferMetadata.rowSet = new string[ResultsMetas.Count, 8];

            // For each file metadata, set the result set variables
            for (int index = 0; index < ResultsMetas.Count; index++)
            {
                TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceFileName] = ResultsMetas[index].srcFileName;
                TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName] = ResultsMetas[index].destFileName;
                TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize] = ResultsMetas[index].srcFileSize.ToString();
                TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize] = ResultsMetas[index].destFileSize.ToString();
                TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus] = ResultsMetas[index].resultStatus;

                if (ResultsMetas[index].lastError != null)
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails] = ResultsMetas[index].lastError.ToString();
                }
                else
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails] = null;
                }

                if (ResultsMetas[index].sourceCompression.Name != null)
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType] = ResultsMetas[index].sourceCompression.Name;
                }
                else
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType] = null;
                }

                if (ResultsMetas[index].targetCompression.Name != null)
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType] = ResultsMetas[index].targetCompression.Name;
                }
                else
                {
                    TransferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType] = null;
                }
            }

            return new SFResultSet(TransferMetadata, new SFStatement(Session), externalCancellationToken);
        }

        /// <summary>
        /// Upload files sequentially or in parallel.
        /// </summary>
        private void upload()
        {
            //Start the upload tasks(for small files upload in parallel using the given parallelism
            //factor, for large file updload sequentially)
            //For each file, using the remote client
            if (0 < LargeFilesMetas.Count)
            {
                Logger.Debug("Start uploading large files");
                foreach (SFFileMetadata fileMetadata in LargeFilesMetas)
                {
                    UploadFilesInSequential(fileMetadata);
                }
                Logger.Debug("End uploading large files");
            }

            if (0 < SmallFilesMetas.Count)
            {
                Logger.Debug("Start uploading small files");
                UploadFilesInParallel(SmallFilesMetas, TransferMetadata.parallel);
                Logger.Debug("End uploading small files");
            }
        }

        /// <summary>
        /// Upload files sequentially or in parallel.
        /// </summary>
        private async Task uploadAsync(CancellationToken cancellationToken)
        {
            //Start the upload tasks(for small files upload in parallel using the given parallelism
            //factor, for large file updload sequentially)
            //For each file, using the remote client
            if (0 < LargeFilesMetas.Count)
            {
                Logger.Debug("Start uploading large files");
                foreach (SFFileMetadata fileMetadata in LargeFilesMetas)
                {
                    await UploadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                }
                Logger.Debug("End uploading large files");
            }

            if (0 < SmallFilesMetas.Count)
            {
                Logger.Debug("Start uploading small files");
                await UploadFilesInParallelAsync(SmallFilesMetas, TransferMetadata.parallel, cancellationToken).ConfigureAwait(false);
                Logger.Debug("End uploading small files");
            }
        }


        /// <summary>
        /// Download files sequentially or in parallel.
        /// </summary>
        private void download()
        {
            //Start the download tasks(for small files download in parallel using the given parallelism
            //factor, for large file download sequentially)
            //For each file, using the remote client
            if (0 < LargeFilesMetas.Count)
            {
                Logger.Debug("Start uploading large files");
                foreach (SFFileMetadata fileMetadata in LargeFilesMetas)
                {
                    DownloadFilesInSequential(fileMetadata);
                }
                Logger.Debug("End uploading large files");
            }
            if (0 < SmallFilesMetas.Count)
            {
                Logger.Debug("Start uploading small files");
                DownloadFilesInParallel(SmallFilesMetas, TransferMetadata.parallel);
                Logger.Debug("End uploading small files");
            }
        }

        /// <summary>
        /// Download files sequentially or in parallel.
        /// </summary>
        private async Task downloadAsync(CancellationToken cancellationToken)
        {
            //Start the download tasks(for small files download in parallel using the given parallelism
            //factor, for large file download sequentially)
            //For each file, using the remote client
            if (0 < LargeFilesMetas.Count)
            {
                Logger.Debug("Start uploading large files");
                foreach (SFFileMetadata fileMetadata in LargeFilesMetas)
                {
                    await DownloadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                }
                Logger.Debug("End uploading large files");
            }
            if (0 < SmallFilesMetas.Count)
            {
                Logger.Debug("Start uploading small files");
                await DownloadFilesInParallelAsync(SmallFilesMetas, TransferMetadata.parallel, cancellationToken).ConfigureAwait(false);
                Logger.Debug("End uploading small files");
            }
        }

        /// <summary>
        /// Get the presigned URL and update the file metadata.
        /// </summary>
        private void updatePresignedUrl()
        {
            // Presigned url only applies to GCS
            if (TransferMetadata.stageInfo.locationType == "GCS")
            {
                if (CommandTypes.UPLOAD == CommandType)
                {
                    foreach (SFFileMetadata fileMeta in FilesMetas)
                    {
                        string filePathToReplace = getFilePathFromPutCommand(Query);
                        string fileNameToReplaceWith = fileMeta.destFileName;
                        string queryWithSingleFile = Query;
                        queryWithSingleFile = queryWithSingleFile.Replace(filePathToReplace, fileNameToReplaceWith);

                        SFStatement sfStatement = new SFStatement(Session);
                        sfStatement.isPutGetQuery = true;

                        PutGetExecResponse response =
                            sfStatement.ExecuteHelper<PutGetExecResponse, PutGetResponseData>(
                                0,
                                queryWithSingleFile,
                                null,
                                false);

                        fileMeta.stageInfo = response.data.stageInfo;
                        fileMeta.presignedUrl = response.data.stageInfo.presignedUrl;
                    }
                }
                else if (CommandTypes.DOWNLOAD == CommandType)
                {
                    for (int index = 0; index < FilesMetas.Count; index++)
                    {
                        FilesMetas[index].presignedUrl = TransferMetadata.presignedUrls[index];
                    }
                }
            }
        }

        /// <summary>
        /// Get the presigned URL async method and update the file metadata.
        /// </summary>
        internal async Task updatePresignedUrlAsync(CancellationToken cancellationToken)
        {
            // Presigned url only applies to GCS
            if (TransferMetadata.stageInfo.locationType == "GCS")
            {
                if (CommandTypes.UPLOAD == CommandType)
                {
                    foreach (SFFileMetadata fileMeta in FilesMetas)
                    {
                        string filePathToReplace = getFilePathFromPutCommand(Query);
                        string fileNameToReplaceWith = fileMeta.destFileName;
                        string queryWithSingleFile = Query;
                        queryWithSingleFile = queryWithSingleFile.Replace(filePathToReplace, fileNameToReplaceWith);

                        SFStatement sfStatement = new SFStatement(Session);
                        sfStatement.isPutGetQuery = true;

                        PutGetExecResponse response = await
                            sfStatement.ExecuteAsyncHelper<PutGetExecResponse, PutGetResponseData>(
                                0,
                                queryWithSingleFile,
                                null,
                                false,
                                cancellationToken).ConfigureAwait(false);

                        fileMeta.stageInfo = response.data.stageInfo;
                        fileMeta.presignedUrl = response.data.stageInfo.presignedUrl;
                    }
                }
                else if (CommandTypes.DOWNLOAD == CommandType)
                {
                    for (int index = 0; index < FilesMetas.Count; index++)
                    {
                        FilesMetas[index].presignedUrl = TransferMetadata.presignedUrls[index];
                    }
                }
            }
        }

        /// <summary>
        /// Obtain the file path from the PUT query.
        /// </summary>
        /// <param name="query">The query containing the file path</param>
        /// <returns>The file path contained by the query</returns>
        internal static string getFilePathFromPutCommand(string query)
        {
            // Extract file path from PUT command:
            // E.g. "PUT file://C:<path-to-file> @DB.SCHEMA.%TABLE;"
            int startIndex = query.IndexOf("file://") + "file://".Length;
            int endIndex = query.Substring(startIndex).IndexOf('@') - 1;
            string filePath = query.Substring(startIndex, endIndex).TrimEnd();

            // Check if file path contains an enclosing (') char
            if (filePath[filePath.Length - 1] == '\'')
            {
                filePath = filePath.Substring(0, filePath.Length - 1);
            }
            return filePath;
        }

        /// <summary>
        /// Initialize the encryption materials for file encryption.
        /// </summary>
        private void initEncryptionMaterial()
        {
            if (CommandTypes.UPLOAD == CommandType)
            {
                if (TransferMetadata.stageInfo.isClientSideEncrypted)
                {
                    EncryptionMaterials.Add(TransferMetadata.encryptionMaterial[0]);
                }
            }
        }

        /// <summary>
        /// Initialize the file metadata of each file to be uploaded/downloaded.
        /// </summary>
        /// <param name="files">List of files to obtain metadata from</param>
        private void initFileMetadata(
            List<string> files)
        {
            if (CommandTypes.UPLOAD == CommandType)
            {
                foreach (string file in files)
                {
                    FileInfo fileInfo = new FileInfo(file);

                    //  Retrieve / Compute the file actual compression type for each file in the list(most work is for auto - detect)
                    string fileName = fileInfo.Name;
                    SFFileCompressionTypes.SFFileCompressionType compressionType;

                    if (TransferMetadata.autoCompress &&
                        TransferMetadata.sourceCompression.Equals(COMPRESSION_AUTO_DETECT))
                    {
                        // Auto-detect source compression type
                        // Will return NONE if no matching type is found
                        compressionType = SFFileCompressionTypes.GuessCompressionType(file);
                        Logger.Debug($"File compression detected as {compressionType.Name} for: {file}");
                    }
                    else
                    {
                        // User defined source compression type
                        compressionType =
                            SFFileCompressionTypes.LookUpByName(TransferMetadata.sourceCompression);
                    }

                    // Verify that the compression type is supported
                    if (!compressionType.IsSupported)
                    {
                        //   SqlState.FEATURE_NOT_SUPPORTED = 0A000
                        throw new SnowflakeDbException("0A000", SFError.INTERNAL_ERROR, compressionType.Name);
                    }

                    SFFileMetadata fileMetadata = new SFFileMetadata()
                    {
                        srcFilePath = file,
                        srcFileName = fileName,
                        srcFileSize = (memoryStream == null) ? fileInfo.Length : memoryStream.Length,
                        stageInfo = TransferMetadata.stageInfo,
                        overwrite = TransferMetadata.overwrite,
                        // Need to compress before sending only if autoCompress is On and the file is
                        // not compressed yet
                        requireCompress = (
                            TransferMetadata.autoCompress &&
                            (SFFileCompressionTypes.NONE.Equals(compressionType))),
                        sourceCompression = compressionType,
                        presignedUrl = TransferMetadata.stageInfo.presignedUrl,
                        // If the file is under the threshold, don't upload in chunks, set parallel to 1
                        parallel = (memoryStream == null) && (fileInfo.Length > TransferMetadata.threshold) ?
                            TransferMetadata.parallel : 1,
                        memoryStream = memoryStream,
                        proxyCredentials = null,
                        MaxBytesInMemory = GetFileTransferMaxBytesInMemory(),
                        _operationType = CommandTypes.UPLOAD
                    };

                    /// The storage client used to upload data from files or streams
                    /// This is only needed for remote storage types
                    if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                    {
                        fileMetadata.client = SFRemoteStorageUtil.GetRemoteStorage(TransferMetadata);
                    }

                    if (!fileMetadata.requireCompress)
                    {
                        // The file is already compressed
                        fileMetadata.targetCompression = fileMetadata.sourceCompression;
                        fileMetadata.destFileName = fileName;
                    }
                    else
                    {
                        // The file will need to be compressed using gzip
                        fileMetadata.targetCompression = SFFileCompressionTypes.GZIP;
                        fileMetadata.destFileName = fileName + SFFileCompressionTypes.GZIP.FileExtension;
                    }

                    if (EncryptionMaterials.Count > 0)
                    {
                        fileMetadata.encryptionMaterial = EncryptionMaterials[0];
                    }

                    if (Session.properties.ContainsKey(SFSessionProperty.PROXYHOST))
                    {
                        string host, port, user, password;
                        Session.properties.TryGetValue(SFSessionProperty.PROXYHOST, out host);
                        Session.properties.TryGetValue(SFSessionProperty.PROXYPORT, out port);
                        Session.properties.TryGetValue(SFSessionProperty.PROXYUSER, out user);
                        Session.properties.TryGetValue(SFSessionProperty.PROXYPASSWORD, out password);


                        fileMetadata.proxyCredentials = new ProxyCredentials()
                        {
                            ProxyHost = host,
                            ProxyPort = Convert.ToInt32(port),
                            ProxyUser = host,
                            ProxyPassword = password
                        };
                    }

                    FilesMetas.Add(fileMetadata);
                }
            }
            else if (CommandTypes.DOWNLOAD == CommandType)
            {
                for (int index = 0; index < files.Count; index++)
                {
                    string file = files[index];
                    SFFileMetadata fileMetadata = new SFFileMetadata()
                    {
                        srcFileName = file,
                        destFileName = file.Split('/').Last(),
                        localLocation = TransferMetadata.localLocation,
                        stageInfo = TransferMetadata.stageInfo,
                        overwrite = TransferMetadata.overwrite,
                        presignedUrl = TransferMetadata.stageInfo.presignedUrl,
                        parallel = TransferMetadata.parallel,
                        encryptionMaterial = index < TransferMetadata.encryptionMaterial.Count
                            ? TransferMetadata.encryptionMaterial[index]
                            : null,
                        MaxBytesInMemory = GetFileTransferMaxBytesInMemory(),
                        _operationType = CommandTypes.DOWNLOAD
                    };

                    /// The storage client used to download data from files or streams
                    /// This is only needed for remote storage types
                    if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                    {
                        fileMetadata.client = SFRemoteStorageUtil.GetRemoteStorage(TransferMetadata);

                        FileHeader fileHeader = fileMetadata.client.GetFileHeader(fileMetadata);

                        if (fileHeader != null)
                        {
                            fileMetadata.srcFileSize = fileHeader.contentLength;
                            fileMetadata.encryptionMetadata = fileHeader.encryptionMetadata;
                        }
                    }

                    FilesMetas.Add(fileMetadata);
                }
            }
        }

        private int GetFileTransferMaxBytesInMemory()
        {
            if (!Session.properties.TryGetValue(SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD, out var maxBytesInMemoryString))
            {
                return FileTransferConfiguration.DefaultMaxBytesInMemory;
            }
            if (string.IsNullOrEmpty(maxBytesInMemoryString))
            {
                return FileTransferConfiguration.DefaultMaxBytesInMemory;
            }
            try
            {
                return int.Parse(maxBytesInMemoryString);
            }
            catch (Exception)
            {
                Logger.Warn("Default for FILE_TRANSFER_MEMORY_THRESHOLD used due to invalid session value.");
                return FileTransferConfiguration.DefaultMaxBytesInMemory;
            }
        }

        /// <summary>
        /// Expand the wildcards if any to generate the list of paths for all files matched by the wildcards.
        /// Also replace the relative paths to the absolute paths for the files if needed.
        /// </summary>
        /// <param name="location">The path to expand</param>
        /// <returns>The list of file matching the input location</returns>
        /// <exception cref="DirectoryNotFoundException">Directory not found. Could not find a part of the pat </exception>
        /// <exception cref="FileNotFoundException">File not found or the path is pointing to a Directory</exception>
        private List<string> expandFileNames(string location)
        {
            location = ExpandHomeDirectoryIfNeeded(location);
            var fileName = Path.GetFileName(location);
            var directoryName = Path.GetDirectoryName(location);
            var foundDirectories = ExpandDirectories(directoryName);
            var filePaths = new List<string>();

            if (ContainsWildcard(fileName))
            {
                foreach (var directory in foundDirectories)
                {
                    var ext = Path.GetExtension(fileName);
                    /*
                     * We have to check that the extension format is exactly 4 characters (e.g. .txt) as there is
                     * an anomaly within .NET Framework usage of GetFiles method when using three-character
                     * file extension (without a dot) which returns files with extensions that begin with the
                     * specified pattern. For example searching for "*.xls" returns both "book.xls" and "book.xlsx".
                     */
                    if (4 == ext.Length && fileName.Contains('*'))
                    {
                        var potentialMatches =
                            Directory.GetFiles(
                                directory,
                                fileName,
                                SearchOption.TopDirectoryOnly);
                        filePaths.AddRange(potentialMatches.Where(potentialMatch => potentialMatch.EndsWith(ext)));
                    }
                    else
                    {
                        filePaths.AddRange(
                            Directory.GetFiles(
                                directory,
                                fileName,
                                SearchOption.TopDirectoryOnly));
                    }
                }
            }
            else
            {
                // No wildcard in the filename
                foreach (var directory in foundDirectories)
                {
                    var fullPath = Path.GetFullPath(directory + fileName);
                    if (IsDirectory(fullPath))
                    {
                        throw new FileNotFoundException(
                            "Directories not supported, you need to provide a file path", fullPath);
                    }
                    filePaths.Add(fullPath);
                }
            }

            if (Logger.IsDebugEnabled())
            {
                Logger.Debug("Expand " + location + " into: ");
                foreach (var filepath in filePaths)
                {
                    Logger.Debug("\t" + filepath);
                }
            }

            return filePaths;
        }

        /// <summary>
        /// Expand the wildcards in the directory path to generate the list of directories to be searched for the files.
        /// </summary>
        /// <param name="directoryPath">The path to expand</param>
        private static IEnumerable<string> ExpandDirectories(string directoryPath)
        {
            if (string.IsNullOrEmpty(directoryPath))
            {
                return new List<string> { Directory.GetCurrentDirectory() + Path.DirectorySeparatorChar };
            }
            if (!ContainsWildcard(directoryPath))
            {
                return new List<string> { Path.GetFullPath(directoryPath) + Path.DirectorySeparatorChar };
            }

            var pathParts = directoryPath.Split(Path.DirectorySeparatorChar);
            var resolvedPaths = new List<string>();

            bool firstPass = true;

            foreach (var part in pathParts)
            {
                if (ContainsWildcard(part))
                {
                    // Directory containing the wildcard is the first one in the path
                    if (firstPass)
                    {
                        resolvedPaths.Add(Directory.GetCurrentDirectory());
                    }

                    var tempPaths = new List<string>();
                    foreach (var location in resolvedPaths)
                    {
                        var foundDirectories = Directory.GetDirectories(location, part, SearchOption.TopDirectoryOnly);
                        foundDirectories = foundDirectories.Select(s => s + Path.DirectorySeparatorChar).ToArray();
                        tempPaths.AddRange(foundDirectories);
                    }

                    resolvedPaths = tempPaths;
                }
                else
                {
                    if (resolvedPaths.Count == 0)
                    {
                        var pathBeginning = "";
                        if ((Environment.OSVersion.Platform == PlatformID.Unix ||
                             Environment.OSVersion.Platform == PlatformID.MacOSX) &&
                            directoryPath.StartsWith(Path.DirectorySeparatorChar.ToString()))
                        {
                            pathBeginning = Path.DirectorySeparatorChar.ToString();
                        }

                        pathBeginning += $"{part}{Path.DirectorySeparatorChar.ToString()}";
                        resolvedPaths.Add(pathBeginning);
                    }
                    else
                    {
                        resolvedPaths = resolvedPaths.Select(s => s + (part + Path.DirectorySeparatorChar)).ToList();
                    }
                }

                firstPass = false;
            }

            return resolvedPaths;
        }

        /// <summary>
        /// Expand the home directory if needed.
        /// </summary>
        /// <param name="directoryPath">The path to expand</param>
        private static string ExpandHomeDirectoryIfNeeded(string directoryPath)
        {
            if (!directoryPath.Contains('~')) return directoryPath;

            var homePath = (Environment.OSVersion.Platform == PlatformID.Unix ||
                            Environment.OSVersion.Platform == PlatformID.MacOSX)
                ? Environment.GetEnvironmentVariable("HOME")
                : Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

            return directoryPath.Replace("~", homePath);
        }

        /// <summary>
        /// Compress a file using the given file metadata (file path, compression type, etc...) and
        /// update the metadata accordingly after the compression is finished.
        /// </summary>
        /// <param name="fileMetadata">The metadata for the file to compress.</param>
        private void compressFileWithGzip(SFFileMetadata fileMetadata)
        {
            FileInfo fileToCompress = new FileInfo(fileMetadata.srcFilePath);
            fileMetadata.realSrcFilePath = Path.Combine(fileMetadata.tmpDir, fileMetadata.srcFileName + "_c.gz");

            using (FileStream originalFileStream = fileToCompress.OpenRead())
            {
                if ((File.GetAttributes(fileToCompress.FullName) &
                   FileAttributes.Hidden) != FileAttributes.Hidden)
                {
                    using (var compressedFileStream = FileOperations.Instance.Create(fileMetadata.realSrcFilePath))
                    {
                        using (GZipStream compressionStream =
                            new GZipStream(compressedFileStream, CompressionMode.Compress))
                        {
                            originalFileStream.CopyTo(compressionStream);
                        }
                    }

                    Logger.Debug($"Compressed {fileToCompress.Name} to {fileMetadata.realSrcFilePath}");
                    FileInfo destInfo = new FileInfo(fileMetadata.realSrcFilePath);
                    fileMetadata.destFileSize = destInfo.Length;
                }
            }
        }

        /// <summary>
        /// Get digest and size of file to be uploaded.
        /// </summary>
        /// <param name="fileMetadata">The metadata for the file to get digest.</param>
        private void getDigestAndSizeForFile(SFFileMetadata fileMetadata)
        {
            using (SHA256 SHA256 = SHA256Managed.Create())
            {
                if (fileMetadata.memoryStream != null)
                {
                    fileMetadata.memoryStream.Position = 0;
                    fileMetadata.sha256Digest = Convert.ToBase64String(SHA256.ComputeHash(fileMetadata.memoryStream));
                    fileMetadata.uploadSize = memoryStream.Length;
                }
                else
                {
                    using (FileStream fileStream = File.OpenRead(fileMetadata.realSrcFilePath))
                    {
                        fileMetadata.sha256Digest = Convert.ToBase64String(SHA256.ComputeHash(fileStream));
                        fileMetadata.uploadSize = fileStream.Length;
                    }
                }
            }
        }

        /// <summary>
        /// Renew expired client.
        /// </summary>
        /// <returns>The renewed storage client.</returns>
        private ISFRemoteStorageClient renewExpiredClient(ProxyCredentials proxyCredentials)
        {
            RenewClientMutex.WaitOne();

            SFStatement sfStatement = new SFStatement(Session);

            PutGetExecResponse response =
                sfStatement.ExecuteHelper<PutGetExecResponse, PutGetResponseData>(
                    0,
                    TransferMetadata.command,
                    null,
                    false);

            TransferMetadata = response.data;

            RenewClientMutex.ReleaseMutex();

            return SFRemoteStorageUtil.GetRemoteStorage(response.data, proxyCredentials);
        }

        /// <summary>
        /// Renew expired client.
        /// </summary>
        /// <returns>The renewed storage client.</returns>
        private async Task<ISFRemoteStorageClient> renewExpiredClientAsync(CancellationToken cancellationToken)
        {
            SFStatement sfStatement = new SFStatement(Session);

            PutGetExecResponse response = await
                sfStatement.ExecuteAsyncHelper<PutGetExecResponse, PutGetResponseData>(
                    0,
                    TransferMetadata.command,
                    null,
                    false,
                    cancellationToken).ConfigureAwait(false);

            return SFRemoteStorageUtil.GetRemoteStorage(response.data);
        }

        /// <summary>
        /// Upload a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="fileMetadata">The metadata of the file to upload.</param>
        /// <returns>The result outcome for each file.</returns>
        private void UploadFilesInSequential(
            SFFileMetadata fileMetadata)
        {
            SFFileMetadata resultMetadata = UploadSingleFile(fileMetadata);
            bool breakFlag = false;

            for (int count = 0; count < 10; count++)
            {
                if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
                {
                    fileMetadata.client = renewExpiredClient(fileMetadata.proxyCredentials);
                }
                else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                {
                    updatePresignedUrl();
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

            ResultsMetas.Add(fileMetadata);

            if (INJECT_WAIT_IN_PUT > 0)
            {
                Thread.Sleep(INJECT_WAIT_IN_PUT);
            }
        }

        /// <summary>
        /// Upload a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="fileMetadata">The metadata of the file to upload.</param>
        /// <returns>The result outcome for each file.</returns>
        private async Task UploadFilesInSequentialAsync(
            SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            SFFileMetadata resultMetadata = await UploadSingleFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
            bool breakFlag = false;

            for (int count = 0; count < 10; count++)
            {
                if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
                {
                    fileMetadata.client = await renewExpiredClientAsync(cancellationToken).ConfigureAwait(false);
                }
                else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
                {
                    await updatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);
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

            ResultsMetas.Add(fileMetadata);

            if (INJECT_WAIT_IN_PUT > 0)
            {
                Thread.Sleep(INJECT_WAIT_IN_PUT);
            }
        }

        /// <summary>
        /// Download a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="fileMetadata">The metadata of the file to download.</param>
        /// <returns>The result outcome for each file.</returns>
        private void DownloadFilesInSequential(
            SFFileMetadata fileMetadata)
        {
            SFFileMetadata resultMetadata = DownloadSingleFile(fileMetadata);

            if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
            {
                fileMetadata.client = renewExpiredClient(fileMetadata.proxyCredentials);
            }
            else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
            {
                updatePresignedUrl();
            }

            ResultsMetas.Add(resultMetadata);

            if (INJECT_WAIT_IN_PUT > 0)
            {
                Thread.Sleep(INJECT_WAIT_IN_PUT);
            }
        }

        /// <summary>
        /// Download a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="fileMetadata">The metadata of the file to download.</param>
        /// <returns>The result outcome for each file.</returns>
        private async Task DownloadFilesInSequentialAsync(
            SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            SFFileMetadata resultMetadata = await DownloadSingleFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);

            if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
            {
                fileMetadata.client = await renewExpiredClientAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
            {
                await updatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);
            }

            ResultsMetas.Add(resultMetadata);

            if (INJECT_WAIT_IN_PUT > 0)
            {
                Thread.Sleep(INJECT_WAIT_IN_PUT);
            }
        }

        /// <summary>
        /// Upload a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="filesMetadata">The list of files to upload in parallel.</param>
        /// <param name="parallel">The number of files to upload in parallel.</param>
        /// <returns>The result outcome for each file.</returns>
        private void UploadFilesInParallel(
            List<SFFileMetadata> filesMetadata,
            int parallel)
        {
            var listOfActions = new List<Action>();
            foreach (SFFileMetadata fileMetadata in filesMetadata)
            {
                listOfActions.Add(() => UploadFilesInSequential(fileMetadata));
            }

            var options = new ParallelOptions { MaxDegreeOfParallelism = parallel };
            Parallel.Invoke(options, listOfActions.ToArray());
        }

        /// <summary>
        /// Upload a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="filesMetadata">The list of files to upload in parallel.</param>
        /// <param name="parallel">The number of files to upload in parallel.</param>
        /// <returns>The result outcome for each file.</returns>
        private async Task UploadFilesInParallelAsync(
            List<SFFileMetadata> filesMetadata,
            int parallel,
            CancellationToken cancellationToken)
        {
            var listOfActions = new List<Action>();
            foreach (SFFileMetadata fileMetadata in filesMetadata)
            {
                await UploadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Download a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="filesMetadata">The list of files to download in parallel.</param>
        /// <param name="parallel">The number of files to download in parallel.</param>
        /// <returns>The result outcome for each file.</returns>
        private void DownloadFilesInParallel(
            List<SFFileMetadata> filesMetadata,
            int parallel)
        {
            var listOfActions = new List<Action>();
            foreach (SFFileMetadata fileMetadata in filesMetadata)
            {
                listOfActions.Add(() => DownloadFilesInSequential(fileMetadata));
            }

            var options = new ParallelOptions { MaxDegreeOfParallelism = parallel };
            Parallel.Invoke(options, listOfActions.ToArray());
        }

        /// <summary>
        /// Download a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="filesMetadata">The list of files to download in parallel.</param>
        /// <param name="parallel">The number of files to download in parallel.</param>
        /// <returns>The result outcome for each file.</returns>
        private async Task DownloadFilesInParallelAsync(
            List<SFFileMetadata> filesMetadata,
            int parallel,
            CancellationToken cancellationToken)
        {
            var listOfActions = new List<Action>();
            foreach (SFFileMetadata fileMetadata in filesMetadata)
            {
                await DownloadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
            }

            var options = new ParallelOptions { MaxDegreeOfParallelism = parallel };
            Parallel.Invoke(options, listOfActions.ToArray());
        }

        /// <summary>
        /// Upload a single file.
        /// </summary>
        /// <param name="storageClient">Storage client to upload the file with.</param>
        /// <param name="fileMetadata">The metadata of the file to upload.</param>
        /// <returns>The result outcome.</returns>
        private SFFileMetadata UploadSingleFile(
            SFFileMetadata fileMetadata)
        {
            fileMetadata.realSrcFilePath = fileMetadata.srcFilePath;

            // Create tmp folder to store compressed files
            fileMetadata.tmpDir = GetTemporaryDirectory();

            try
            {
                // Compress the file if needed
                if (fileMetadata.requireCompress)
                {
                    compressFileWithGzip(fileMetadata);
                }

                // Calculate the digest
                getDigestAndSizeForFile(fileMetadata);

                if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                {
                    // Upload the file using the remote client SDK and the file metadata
                    SFRemoteStorageUtil.UploadOneFileWithRetry(fileMetadata);
                }
                else
                {
                    // Upload the file using the local client SDK and the file metadata
                    SFLocalStorageUtil.UploadOneFileWithRetry(fileMetadata);
                }
            }
            catch (Exception ex)
            {
                Logger.Debug("Unhandled exception while uploading file.", ex);
                throw;
            }
            finally
            {
                Directory.Delete(fileMetadata.tmpDir, true);
            }

            return fileMetadata;
        }

        /// <summary>
        /// Upload a single file.
        /// </summary>
        /// <param name="storageClient">Storage client to upload the file with.</param>
        /// <param name="fileMetadata">The metadata of the file to upload.</param>
        /// <returns>The result outcome.</returns>
        private async Task<SFFileMetadata> UploadSingleFileAsync(
            SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            fileMetadata.realSrcFilePath = fileMetadata.srcFilePath;

            // Create tmp folder to store compressed files
            fileMetadata.tmpDir = GetTemporaryDirectory();

            try
            {
                // Compress the file if needed
                if (fileMetadata.requireCompress)
                {
                    compressFileWithGzip(fileMetadata);
                }

                // Calculate the digest
                getDigestAndSizeForFile(fileMetadata);

                if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                {
                    // Upload the file using the remote client SDK and the file metadata
                    await SFRemoteStorageUtil.UploadOneFileWithRetryAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    // Upload the file using the local client SDK and the file metadata
                    SFLocalStorageUtil.UploadOneFileWithRetry(fileMetadata);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("UploadSingleFileAsync encountered an error: " + ex.Message);
                throw;
            }
            finally
            {
                Directory.Delete(fileMetadata.tmpDir, true);
            }

            return fileMetadata;
        }

        /// <summary>
        /// Download a single file.
        /// </summary>
        /// <param name="storageClient">Storage client to download the file with.</param>
        /// <param name="fileMetadata">The metadata of the file to download.</param>
        /// <returns>The result outcome.</returns>
        private SFFileMetadata DownloadSingleFile(
            SFFileMetadata fileMetadata)
        {
            // Create tmp folder to store compressed files
            fileMetadata.tmpDir = GetTemporaryDirectory();

            try
            {
                if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                {
                    // Upload the file using the remote client SDK and the file metadata
                    SFRemoteStorageUtil.DownloadOneFile(fileMetadata);
                }
                else
                {
                    // Upload the file using the local client SDK and the file metadata
                    SFLocalStorageUtil.DownloadOneFile(fileMetadata);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("DownloadSingleFile encountered an error: " + ex.Message);
                throw;
            }
            finally
            {
                Directory.Delete(fileMetadata.tmpDir, true);
            }

            return fileMetadata;
        }

        /// <summary>
        /// Download a single file.
        /// </summary>
        /// <param name="storageClient">Storage client to download the file with.</param>
        /// <param name="fileMetadata">The metadata of the file to download.</param>
        /// <returns>The result outcome.</returns>
        private async Task<SFFileMetadata> DownloadSingleFileAsync(
            SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            // Create tmp folder to store compressed files
            fileMetadata.tmpDir = GetTemporaryDirectory();

            try
            {
                if (StorageClientType.REMOTE == GetStorageClientType(TransferMetadata.stageInfo))
                {
                    // Upload the file using the remote client SDK and the file metadata
                    await SFRemoteStorageUtil.DownloadOneFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    // Upload the file using the local client SDK and the file metadata
                    SFLocalStorageUtil.DownloadOneFile(fileMetadata);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("DownloadSingleFileAsync encountered an error: " + ex.Message);
                throw;
            }
            finally
            {
                Directory.Delete(fileMetadata.tmpDir, true);
            }

            return fileMetadata;
        }

        /// <summary>
        /// Create a temporary directory.
        /// </summary>
        /// <returns>The temporary directory name.</returns>
        /// Referenced from: https://stackoverflow.com/a/278457
        public string GetTemporaryDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }


        /// <summary>
        /// Get the storage client type.
        /// </summary>
        /// <param name="stageInfo">The stage info used to get the stage location type.</param>
        /// <returns>The storage client type.</returns>
        public StorageClientType GetStorageClientType(PutGetStageInfo stageInfo)
        {
            string stageLocationType = stageInfo.locationType;

            if (stageLocationType == LOCAL_FS)
            {
                return StorageClientType.LOCAL;
            }
            else
            {
                return StorageClientType.REMOTE;
            }
        }

        private void initFileMetadataForUpload()
        {
            // Initialize the list of actual files to upload
            List<string> expandedSrcLocations = new List<string>();
            if (memoryStream != null)
            {
                // stream put only support single file
                if (TransferMetadata.src_locations.Count != 1)
                {
                    throw new ArgumentException("Invalid stream put.");
                }
                expandedSrcLocations.Add(TransferMetadata.src_locations[0]);
            }
            else
            {
                foreach (string location in TransferMetadata.src_locations)
                {
                    expandedSrcLocations.AddRange(expandFileNames(location));
                }
            }

            // Initialize each file specific metadata (for example, file path, name and size) and
            // put it in1 of the 2 lists : Small files and large files based on a threshold
            // extracted from the command response
            initFileMetadata(expandedSrcLocations);

            if (expandedSrcLocations.Count == 0)
            {
                throw new ArgumentException("No file found for: " + TransferMetadata.src_locations[0].ToString());
            }
        }

        private static bool IsDirectory(string path)
        {
            var attr = File.GetAttributes(path);
            return attr.HasFlag(FileAttributes.Directory);
        }

        private static bool ContainsWildcard(string str)
        {
            return str.Contains('*') || str.Contains('?');
        }
    }
}
