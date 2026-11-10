using Snowflake.Data.Client;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core;

/// <summary>
/// The status of the file to be uploaded/downloaded.
/// </summary>
internal enum ResultStatus
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
internal class SFFileTransferAgent
{
    /// <summary>
    /// The logger.
    /// </summary>
    private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFFileTransferAgent>();

    /// <summary>
    /// Auto-detect keyword for source compression type auto detection.
    /// </summary>
    private static readonly string COMPRESSION_AUTO_DETECT = "auto_detect";

    /// <summary>
    /// The Snowflake query
    /// </summary>
    private readonly string _query;

    /// <summary>
    /// The Snowflake session
    /// </summary>
    private readonly SFSession _session;

    private readonly IEnvironmentFacade _environmentFacade;

    /// <summary>
    /// The type of transfer either UPLOAD or DOWNLOAD.
    /// </summary>
    private readonly CommandTypes _commandType;

    /// <summary>
    /// The file metadata. Applies to all files being uploaded/downloaded
    /// </summary>
    private PutGetResponseData _transferMetadata;

    /// <summary>
    /// List of metadata for small and large files.
    /// </summary>
    private readonly List<SFFileMetadata> _filesMetas = new();

    private readonly List<SFFileMetadata> _smallFilesMetas = new();
    private readonly List<SFFileMetadata> _largeFilesMetas = new();

    /// <summary>
    /// List of metadata for the resulting file after upload/download.
    /// </summary>
    private readonly List<SFFileMetadata> _resultsMetas = new();

    /// <summary>
    /// List of encryption materials of the files to be uploaded/downloaded.
    /// </summary>
    private readonly List<PutGetEncryptionMaterial> _encryptionMaterials = new();

    /// <summary>
    /// Time to wait before uploading the next file.
    /// </summary>
    private readonly int INJECT_WAIT_IN_PUT = 0;

    /// <summary>
    /// String indicating local storage type.
    /// </summary>
    private const string LOCAL_FS = "LOCAL_FS";

    private const string STREAM_FILE_NAME = "stream";

    /// <summary>
    /// In-memory stream for upload. When <c>BorrowedMemoryStream.Owned</c> is true the driver created
    /// and controls the stream's lifetime; otherwise the stream was provided by the caller.
    /// </summary>
    private readonly BorrowedMemoryStream _memoryStream;

    /// <summary>
    /// Mutex for renewing expired client.
    /// </summary>
    private static readonly Mutex s_renewClientMutex = new();

    /// <summary>
    /// Placeholder threshold value using max long value.
    /// </summary>
    private readonly long DATA_SIZE_THRESHOLD = long.MaxValue;

    /// <summary>
    /// Constructor.
    /// </summary>
    public SFFileTransferAgent(
        string query,
        SFSession session,
        PutGetResponseData responseData,
        IEnvironmentFacade environmentFacade = null)
    {
        _query = query;
        _session = session;
        _transferMetadata = responseData;
        _transferMetadata.threshold = DATA_SIZE_THRESHOLD;
        _commandType = (CommandTypes)Enum.Parse(typeof(CommandTypes), _transferMetadata.command, true);
        _environmentFacade = environmentFacade ?? EnvironmentFacade.Instance;
    }

    public SFFileTransferAgent(
        string query,
        SFSession session,
        PutGetResponseData responseData,
        BorrowedMemoryStream inputStream,
        IEnvironmentFacade environmentFacade = null) : this(query, session, responseData, environmentFacade)
    {
        if (_commandType != CommandTypes.UPLOAD)
            throw new NotSupportedException("In-memory stream for GET queries is not supported.");

        _memoryStream = inputStream;
    }

    /// <summary>
    /// Execute the PUT/GET command.
    /// </summary>
    public void Execute()
    {
        try
        {
            // Initialize the encryption metadata
            InitEncryptionMaterial();

            if (CommandTypes.UPLOAD == _commandType)
            {
                InitFileMetadataForUpload();
            }
            else if (CommandTypes.DOWNLOAD == _commandType)
            {
                InitFileMetadata(_transferMetadata.src_locations);

                Directory.CreateDirectory(_transferMetadata.localLocation);
            }

            // Update the file metadata with GCS presigned URL
            UpdatePresignedUrl();

            foreach (var fileMetadata in _filesMetas)
            {
                // If the file is larger than the threshold, add it to the large files list
                // Otherwise add it to the small files list
                if (fileMetadata.srcFileSize > _transferMetadata.threshold)
                {
                    _largeFilesMetas.Add(fileMetadata);
                }
                else
                {
                    _smallFilesMetas.Add(fileMetadata);
                }
            }

            // Check command type
            if (CommandTypes.UPLOAD == _commandType)
            {
                Upload();
            }
            else if (CommandTypes.DOWNLOAD == _commandType)
            {
                Download();
            }
        }
        catch (Exception e)
        {
            s_logger.Error("Error while transferring file(s): " + e.Message);
            if (e is SnowflakeDbException snowflakeException)
            {
                if (snowflakeException.QueryId == null)
                {
                    snowflakeException.QueryId = _transferMetadata.queryId;
                }

                throw snowflakeException;
            }

            throw new SnowflakeDbException(SFError.IO_ERROR_ON_GETPUT_COMMAND, _transferMetadata.queryId, e);
        }
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        // Initialize the encryption metadata
        InitEncryptionMaterial();

        if (CommandTypes.UPLOAD == _commandType)
        {
            InitFileMetadataForUpload();
        }
        else if (CommandTypes.DOWNLOAD == _commandType)
        {
            InitFileMetadata(_transferMetadata.src_locations);

            Directory.CreateDirectory(_transferMetadata.localLocation);
        }

        // Update the file metadata with GCS presigned URL
        await UpdatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);

        foreach (var fileMetadata in _filesMetas)
        {
            // If the file is larger than the threshold, add it to the large files list
            // Otherwise add it to the small files list
            if (fileMetadata.srcFileSize > _transferMetadata.threshold)
            {
                _largeFilesMetas.Add(fileMetadata);
            }
            else
            {
                _smallFilesMetas.Add(fileMetadata);
            }
        }

        // Check command type
        if (CommandTypes.UPLOAD == _commandType)
        {
            await UploadAsync(cancellationToken).ConfigureAwait(false);
        }
        else if (CommandTypes.DOWNLOAD == _commandType)
        {
            await DownloadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Generate the result set based on the file metadata.
    /// </summary>
    /// <returns>The result set containing file status and info</returns>
    public SFResultSet Result()
    {
        // Set the row count using the number of metadata in the result metas
        _transferMetadata.rowSet = new string[_resultsMetas.Count, 8];

        // For each file metadata, set the result set variables
        for (var index = 0; index < _resultsMetas.Count; index++)
        {
            _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceFileName] = _resultsMetas[index].srcFileName;
            _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName] = _resultsMetas[index].destFileName;
            _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize] =
                _resultsMetas[index].srcFileSize.ToString();
            _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize] =
                _resultsMetas[index].destFileSize.ToString();
            _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus] = _resultsMetas[index].resultStatus;

            if (_resultsMetas[index].lastError != null)
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails] =
                    _resultsMetas[index].lastError.ToString();
            }
            else
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails] = null;
            }

            if (_resultsMetas[index].sourceCompression.Name != null)
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType] =
                    _resultsMetas[index].sourceCompression.Name;
            }
            else
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType] = null;
            }

            if (_resultsMetas[index].targetCompression.Name != null)
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType] =
                    _resultsMetas[index].targetCompression.Name;
            }
            else
            {
                _transferMetadata.rowSet[index, (int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType] = null;
            }
        }

        return new SFResultSet(_transferMetadata, new SFStatement(_session));
    }

    /// <summary>
    /// Upload files sequentially or in parallel.
    /// </summary>
    private void Upload()
    {
        //Start the upload tasks(for small files upload in parallel using the given parallelism
        //factor, for large file updload sequentially)
        //For each file, using the remote client
        if (0 < _largeFilesMetas.Count)
        {
            s_logger.Debug("Start uploading large files");
            foreach (var fileMetadata in _largeFilesMetas)
            {
                UploadFilesInSequential(fileMetadata);
            }

            s_logger.Debug("End uploading large files");
        }

        if (0 < _smallFilesMetas.Count)
        {
            s_logger.Debug("Start uploading small files");
            UploadFilesInParallel(_smallFilesMetas, _transferMetadata.parallel);
            s_logger.Debug("End uploading small files");
        }
    }

    /// <summary>
    /// Upload files sequentially or in parallel.
    /// </summary>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        //Start the upload tasks(for small files upload in parallel using the given parallelism
        //factor, for large file updload sequentially)
        //For each file, using the remote client
        if (0 < _largeFilesMetas.Count)
        {
            s_logger.Debug("Start uploading large files");
            foreach (var fileMetadata in _largeFilesMetas)
            {
                await UploadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
            }

            s_logger.Debug("End uploading large files");
        }

        if (0 < _smallFilesMetas.Count)
        {
            s_logger.Debug("Start uploading small files");
            await UploadFilesInParallelAsync(_smallFilesMetas, cancellationToken).ConfigureAwait(false);
            s_logger.Debug("End uploading small files");
        }
    }


    /// <summary>
    /// Download files sequentially or in parallel.
    /// </summary>
    private void Download()
    {
        //Start the download tasks(for small files download in parallel using the given parallelism
        //factor, for large file download sequentially)
        //For each file, using the remote client
        if (0 < _largeFilesMetas.Count)
        {
            s_logger.Debug("Start uploading large files");
            foreach (var fileMetadata in _largeFilesMetas)
            {
                DownloadFilesInSequential(fileMetadata);
            }

            s_logger.Debug("End uploading large files");
        }

        if (0 < _smallFilesMetas.Count)
        {
            s_logger.Debug("Start uploading small files");
            DownloadFilesInParallel(_smallFilesMetas, _transferMetadata.parallel);
            s_logger.Debug("End uploading small files");
        }
    }

    /// <summary>
    /// Download files sequentially or in parallel.
    /// </summary>
    private async Task DownloadAsync(CancellationToken cancellationToken)
    {
        //Start the download tasks(for small files download in parallel using the given parallelism
        //factor, for large file download sequentially)
        //For each file, using the remote client
        if (0 < _largeFilesMetas.Count)
        {
            s_logger.Debug("Start uploading large files");
            foreach (var fileMetadata in _largeFilesMetas)
            {
                await DownloadFilesInSequentialAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
            }

            s_logger.Debug("End uploading large files");
        }

        if (0 < _smallFilesMetas.Count)
        {
            s_logger.Debug("Start uploading small files");
            await DownloadFilesInParallelAsync(_smallFilesMetas, _transferMetadata.parallel, cancellationToken).ConfigureAwait(false);
            s_logger.Debug("End uploading small files");
        }
    }

    /// <summary>
    /// Get the presigned URL and update the file metadata.
    /// </summary>
    private void UpdatePresignedUrl()
    {
        // Presigned url only applies to GCS
        if (_transferMetadata.stageInfo.locationType != "GCS")
            return;

        if (CommandTypes.DOWNLOAD == _commandType)
        {
            for (var index = 0; index < _filesMetas.Count; index++)
            {
                _filesMetas[index].presignedUrl = _transferMetadata.presignedUrls[index];
            }

            return;
        }

        if (_commandType != CommandTypes.UPLOAD)
            return;

        // Skip entirely in access-token mode: a downscoped token is folder-scoped and already covers every file, so there is nothing per-file to fetch.
        if (_transferMetadata.stageInfo.stageCredentials?.TryGetValue(SFGCSClient.GCS_ACCESS_TOKEN, out var value) == true &&
            !string.IsNullOrEmpty(value))
            return;

        foreach (var fileMeta in _filesMetas)
        {
            var filePathToReplace = GetFilePathFromPutCommand(_query);
            var fileNameToReplaceWith = fileMeta.destFileName;
            var queryWithSingleFile = _query;
            queryWithSingleFile = queryWithSingleFile.Replace(filePathToReplace, fileNameToReplaceWith);

            var sfStatement = new SFStatement(_session)
            {
                isPutGetQuery = true
            };
            var statementContext = StatementContext.Default with { CommandText = queryWithSingleFile };
            var response = sfStatement.ExecuteHelper<PutGetExecResponse, PutGetResponseData>(statementContext, null);

            fileMeta.stageInfo = response.data.stageInfo;
            fileMeta.presignedUrl = response.data.stageInfo.presignedUrl;
        }
    }

    /// <summary>
    /// Get the presigned URL async method and update the file metadata.
    /// </summary>
    private async Task UpdatePresignedUrlAsync(CancellationToken cancellationToken)
    {
        // Presigned url only applies to GCS
        if (_transferMetadata.stageInfo.locationType != "GCS")
            return;

        if (_commandType == CommandTypes.DOWNLOAD)
        {
            for (var index = 0; index < _filesMetas.Count; index++)
            {
                _filesMetas[index].presignedUrl = _transferMetadata.presignedUrls[index];
            }

            return;
        }

        if (_commandType != CommandTypes.UPLOAD)
            return;

        // Skip entirely in access-token mode: a downscoped token is folder-scoped and already covers every file, so there is nothing per-file to fetch.
        if (_transferMetadata.stageInfo.stageCredentials?.TryGetValue(SFGCSClient.GCS_ACCESS_TOKEN, out var value) == true &&
            !string.IsNullOrEmpty(value))
            return;

        foreach (var fileMeta in _filesMetas)
        {
            var filePathToReplace = GetFilePathFromPutCommand(_query);
            var fileNameToReplaceWith = fileMeta.destFileName;
            var queryWithSingleFile = _query;
            queryWithSingleFile = queryWithSingleFile.Replace(filePathToReplace, fileNameToReplaceWith);

            var sfStatement = new SFStatement(_session)
            {
                isPutGetQuery = true
            };

            var statementContext = StatementContext.Default with { CommandText = queryWithSingleFile };
            var response = await sfStatement.ExecuteAsyncHelper<PutGetExecResponse, PutGetResponseData>(statementContext, null, cancellationToken)
                .ConfigureAwait(false);

            fileMeta.stageInfo = response.data.stageInfo;
            fileMeta.presignedUrl = response.data.stageInfo.presignedUrl;
        }
    }

    /// <summary>
    /// Obtain the file path from the PUT query.
    /// </summary>
    /// <param name="query">The query containing the file path</param>
    /// <returns>The file path contained by the query</returns>
    internal static string GetFilePathFromPutCommand(string query)
    {
        // Extract file path from PUT command:
        // E.g. "PUT file://C:<path-to-file> @DB.SCHEMA.%TABLE;"
        var startIndex = query.IndexOf("file://") + "file://".Length;
        var endIndex = query.Substring(startIndex).IndexOf('@') - 1;
        var filePath = query.Substring(startIndex, endIndex).TrimEnd();

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
    private void InitEncryptionMaterial()
    {
        if (CommandTypes.UPLOAD == _commandType)
        {
            if (_transferMetadata.stageInfo.isClientSideEncrypted)
            {
                _encryptionMaterials.Add(_transferMetadata.encryptionMaterial[0]);
            }
        }
    }

    private (string Name, long Length, SFFileCompressionTypes.SFFileCompressionType CompressionType) GetCompressionData(string name)
    {
        string resultName;
        long resultLength;
        if (_memoryStream?.Owned == false)
        {
            resultName = $"{STREAM_FILE_NAME}.{Path.GetFileName(name)}";
            resultLength = _memoryStream.Length;
        }
        else
        {
            var fileInfo = new FileInfo(name);

            //  Retrieve / Compute the file actual compression type for each file in the list(most work is for auto - detect)
            resultName = fileInfo.Name;
            resultLength = _memoryStream?.Length ?? fileInfo.Length;
        }

        SFFileCompressionTypes.SFFileCompressionType compressionType;

        if (_transferMetadata.autoCompress &&
            _transferMetadata.sourceCompression.Equals(COMPRESSION_AUTO_DETECT))
        {
            // Auto-detect source compression type
            // Will return NONE if no matching type is found
            compressionType = _memoryStream?.Owned == false
                ? SFFileCompressionTypes.GuessCompressionType(_memoryStream)
                : SFFileCompressionTypes.GuessCompressionType(name);

            s_logger.Debug($"File compression detected as {compressionType.Name} for: {name}");
        }
        else
        {
            // User defined source compression type
            compressionType =
                SFFileCompressionTypes.LookUpByName(_transferMetadata.sourceCompression);
        }

        // Verify that the compression type is supported
        if (!compressionType.IsSupported)
        {
            //   SqlState.FEATURE_NOT_SUPPORTED = 0A000
            throw new SnowflakeDbException("0A000", SFError.INTERNAL_ERROR, compressionType.Name);
        }

        return (resultName, resultLength, compressionType);
    }

    /// <summary>
    /// Initialize the file metadata of each file to be uploaded/downloaded.
    /// </summary>
    /// <param name="files">List of files to obtain metadata from</param>
    private void InitFileMetadata(List<string> files)
    {
        if (CommandTypes.UPLOAD == _commandType)
        {
            foreach (var file in files)
            {
                var (sourceName, length, compressionType) = GetCompressionData(file);

                var fileMetadata = new SFFileMetadata
                {
                    srcFilePath = file,
                    srcFileName = sourceName,
                    srcFileSize = length,
                    stageInfo = _transferMetadata.stageInfo,
                    overwrite = _transferMetadata.overwrite,
                    // Need to compress before sending only if autoCompress is On and the file is
                    // not compressed yet
                    requireCompress = _transferMetadata.autoCompress && SFFileCompressionTypes.NONE.Equals(compressionType),
                    sourceCompression = compressionType,
                    presignedUrl = _transferMetadata.stageInfo.presignedUrl,
                    // If the file is under the threshold, don't upload in chunks, set parallel to 1
                    parallel = (_memoryStream == null && length > _transferMetadata.threshold) ? _transferMetadata.parallel : 1,
                    memoryStream = _memoryStream,
                    canCompressSpillToFile = _memoryStream?.Owned != false || _environmentFacade.GetBool(EnvVars.PutDisableInMemoryCompress),
                    proxyCredentials = null,
                    MaxBytesInMemory = GetFileTransferMaxBytesInMemory(),
                    _operationType = CommandTypes.UPLOAD
                };

                /// The storage client used to upload data from files or streams
                /// This is only needed for remote storage types
                if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
                {
                    fileMetadata.client = SFRemoteStorageUtil.GetRemoteStorage(_transferMetadata, tlsProtocols: _session.TlsProtocols, tlsProtocolsExplicitlyRequested: _session.TlsProtocolsExplicitlyRequested);
                }

                if (!fileMetadata.requireCompress)
                {
                    // The file is already compressed
                    fileMetadata.targetCompression = fileMetadata.sourceCompression;
                    fileMetadata.destFileName = sourceName;
                }
                else
                {
                    // The file will need to be compressed using gzip
                    fileMetadata.targetCompression = SFFileCompressionTypes.GZIP;
                    fileMetadata.destFileName = sourceName + SFFileCompressionTypes.GZIP.FileExtension;
                }

                if (_encryptionMaterials.Count > 0)
                {
                    fileMetadata.encryptionMaterial = _encryptionMaterials[0];
                }

                if (_session.properties.ContainsKey(SFSessionProperty.PROXYHOST))
                {
                    string host, port, user, password;
                    _session.properties.TryGetValue(SFSessionProperty.PROXYHOST, out host);
                    _session.properties.TryGetValue(SFSessionProperty.PROXYPORT, out port);
                    _session.properties.TryGetValue(SFSessionProperty.PROXYUSER, out user);
                    _session.properties.TryGetValue(SFSessionProperty.PROXYPASSWORD, out password);


                    fileMetadata.proxyCredentials = new ProxyCredentials()
                    {
                        ProxyHost = host,
                        ProxyPort = Convert.ToInt32(port),
                        ProxyUser = host,
                        ProxyPassword = password
                    };
                }

                _filesMetas.Add(fileMetadata);
            }
        }
        else if (CommandTypes.DOWNLOAD == _commandType)
        {
            for (var index = 0; index < files.Count; index++)
            {
                var file = files[index];
                var fileMetadata = new SFFileMetadata()
                {
                    srcFileName = file,
                    destFileName = file.Split('/').Last(),
                    localLocation = _transferMetadata.localLocation,
                    stageInfo = _transferMetadata.stageInfo,
                    overwrite = _transferMetadata.overwrite,
                    presignedUrl = _transferMetadata.stageInfo.presignedUrl,
                    parallel = _transferMetadata.parallel,
                    encryptionMaterial = index < _transferMetadata.encryptionMaterial.Count
                        ? _transferMetadata.encryptionMaterial[index]
                        : null,
                    MaxBytesInMemory = GetFileTransferMaxBytesInMemory(),
                    _operationType = CommandTypes.DOWNLOAD
                };

                /// The storage client used to download data from files or streams
                /// This is only needed for remote storage types
                if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
                {
                    fileMetadata.client = SFRemoteStorageUtil.GetRemoteStorage(_transferMetadata, tlsProtocols: _session.TlsProtocols, tlsProtocolsExplicitlyRequested: _session.TlsProtocolsExplicitlyRequested);

                    var fileHeader = fileMetadata.client.GetFileHeader(fileMetadata);

                    if (fileHeader != null)
                    {
                        fileMetadata.srcFileSize = fileHeader.contentLength;
                        fileMetadata.encryptionMetadata = fileHeader.encryptionMetadata;
                    }
                }

                _filesMetas.Add(fileMetadata);
            }
        }
    }

    private int GetFileTransferMaxBytesInMemory()
    {
        if (!_session.properties.TryGetValue(SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD, out var maxBytesInMemoryString))
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
            s_logger.Warn("Default for FILE_TRANSFER_MEMORY_THRESHOLD used due to invalid session value.");
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
    private static List<string> ExpandFileNames(string location)
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

        if (s_logger.IsDebugEnabled())
        {
            s_logger.Debug("Expand " + location + " into: ");
            foreach (var filepath in filePaths)
            {
                s_logger.Debug("\t" + filepath);
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

        var firstPass = true;

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
            ? EnvironmentFacade.Instance.GetString(EnvVars.Home)
            : Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

        return directoryPath.Replace("~", homePath);
    }

    /// <summary>
    /// Compress the in-memory stream using GZip and update file metadata with the compressed result.
    /// </summary>
    /// <param name="fileMetadata">The metadata for the file to compress.</param>
    /// <param name="uncompressedStream">Origin stream.</param>
    private static void CompressInMemoryWithGzip(SFFileMetadata fileMetadata, out BorrowedMemoryStream uncompressedStream)
    {
        var compressed = new MemoryStream();
        fileMetadata.memoryStream.Position = 0;
        using (var gzip = new GZipStream(compressed, CompressionMode.Compress, leaveOpen: true))
        {
            fileMetadata.memoryStream.CopyTo(gzip);
            uncompressedStream = fileMetadata.memoryStream;
        }

        compressed.Position = 0;
        fileMetadata.memoryStream = compressed.GetBorrowedStream(true);
        fileMetadata.destFileSize = compressed.Length;
    }

    /// <summary>
    /// Compress a file using the given file metadata (file path, compression type, etc...) and
    /// update the metadata accordingly after the compression is finished.
    /// </summary>
    /// <param name="fileMetadata">The metadata for the file to compress.</param>
    /// <param name="memoryStream">Origin memory stream</param>
    private static void CompressFileWithGzip(SFFileMetadata fileMetadata, out BorrowedMemoryStream memoryStream)
    {
        if (!fileMetadata.canCompressSpillToFile)
        {
            CompressInMemoryWithGzip(fileMetadata, out memoryStream);
            return;
        }

        memoryStream = fileMetadata.memoryStream;
        fileMetadata.realSrcFilePath = Path.Combine(fileMetadata.tmpDir, fileMetadata.srcFileName + "_c.gz");

        if (memoryStream == null)
        {
            var fileToCompress = new FileInfo(fileMetadata.srcFilePath);
            if ((File.GetAttributes(fileToCompress.FullName) & FileAttributes.Hidden) == FileAttributes.Hidden) return;

            using var originalFileStream = fileToCompress.OpenRead();
            using var compressedFileStream = FileOperations.Instance.Create(fileMetadata.realSrcFilePath);
            using var compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress);
            originalFileStream.CopyTo(compressionStream);
        }
        else
        {
            memoryStream.Position = 0;
            using var compressedFileStream = FileOperations.Instance.Create(fileMetadata.realSrcFilePath);
            using var compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress);
            memoryStream.CopyTo(compressionStream);
            fileMetadata.memoryStream = null;
        }

        s_logger.Debug($"Compressed {fileMetadata.srcFileName} to {fileMetadata.realSrcFilePath}");
        var destInfo = new FileInfo(fileMetadata.realSrcFilePath);
        fileMetadata.destFileSize = destInfo.Length;
    }

    /// <summary>
    /// Get digest and size of file to be uploaded.
    /// </summary>
    /// <param name="fileMetadata">The metadata for the file to get digest.</param>
    private static void GetDigestAndSizeForFile(SFFileMetadata fileMetadata)
    {
#pragma warning disable SYSLIB0021 // TODO SNOW-3662960
        using var sha256 = SHA256Managed.Create();
        if (fileMetadata.memoryStream != null)
        {
            fileMetadata.memoryStream.Position = 0;
            fileMetadata.sha256Digest = Convert.ToBase64String(sha256.ComputeHash(fileMetadata.memoryStream));
            fileMetadata.uploadSize = fileMetadata.memoryStream.Length;
        }
        else
        {
            using var fileStream = File.OpenRead(fileMetadata.realSrcFilePath);
            fileMetadata.sha256Digest = Convert.ToBase64String(sha256.ComputeHash(fileStream));
            fileMetadata.uploadSize = fileStream.Length;
        }
    }

    /// <summary>
    /// Renew expired client.
    /// </summary>
    /// <returns>The renewed storage client.</returns>
    private ISFRemoteStorageClient RenewExpiredClient(ProxyCredentials proxyCredentials)
    {
        s_renewClientMutex.WaitOne();

        var sfStatement = new SFStatement(_session);
        var statementContext = StatementContext.Default with { CommandText = _transferMetadata.command };
        var response = sfStatement.ExecuteHelper<PutGetExecResponse, PutGetResponseData>(statementContext, null);

        _transferMetadata = response.data;

        s_renewClientMutex.ReleaseMutex();

        return SFRemoteStorageUtil.GetRemoteStorage(response.data, proxyCredentials, _session.TlsProtocols, _session.TlsProtocolsExplicitlyRequested);
    }

    /// <summary>
    /// Renew expired client.
    /// </summary>
    /// <returns>The renewed storage client.</returns>
    private async Task<ISFRemoteStorageClient> RenewExpiredClientAsync(CancellationToken cancellationToken)
    {
        var sfStatement = new SFStatement(_session);
        var statementContext = StatementContext.Default with { CommandText = _transferMetadata.command };
        var response = await
            sfStatement.ExecuteAsyncHelper<PutGetExecResponse, PutGetResponseData>(
                statementContext,
                null,
                cancellationToken).ConfigureAwait(false);

        return SFRemoteStorageUtil.GetRemoteStorage(response.data, tlsProtocols: _session.TlsProtocols, tlsProtocolsExplicitlyRequested: _session.TlsProtocolsExplicitlyRequested);
    }

    /// <summary>
    /// Upload a list of files in parallel using the given parallelization factor.
    /// </summary>
    /// <param name="fileMetadata">The metadata of the file to upload.</param>
    /// <returns>The result outcome for each file.</returns>
    private void UploadFilesInSequential(
        SFFileMetadata fileMetadata)
    {
        var resultMetadata = UploadSingleFile(fileMetadata);
        var breakFlag = false;

        for (var count = 0; count < 10; count++)
        {
            if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
            {
                fileMetadata.client = RenewExpiredClient(fileMetadata.proxyCredentials);
            }
            else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
            {
                UpdatePresignedUrl();
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

        _resultsMetas.Add(fileMetadata);

        if (INJECT_WAIT_IN_PUT > 0)
        {
            Thread.Sleep(INJECT_WAIT_IN_PUT);
        }
    }

    /// <summary>
    /// Upload a list of files in parallel using the given parallelization factor.
    /// </summary>
    /// <param name="fileMetadata">The metadata of the file to upload.</param>
    /// <param name="cancellationToken">Cancellation support.</param>
    /// <returns>The result outcome for each file.</returns>
    private async Task UploadFilesInSequentialAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
    {
        var resultMetadata = await UploadSingleFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);
        var breakFlag = false;

        for (var count = 0; count < 10; count++)
        {
            if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
            {
                fileMetadata.client = await RenewExpiredClientAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
            {
                await UpdatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);
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

        _resultsMetas.Add(fileMetadata);

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
        var resultMetadata = DownloadSingleFile(fileMetadata);

        if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
        {
            fileMetadata.client = RenewExpiredClient(fileMetadata.proxyCredentials);
        }
        else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
        {
            UpdatePresignedUrl();
        }

        _resultsMetas.Add(resultMetadata);

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
        var resultMetadata = await DownloadSingleFileAsync(fileMetadata, cancellationToken).ConfigureAwait(false);

        if (resultMetadata.resultStatus == ResultStatus.RENEW_TOKEN.ToString())
        {
            fileMetadata.client = await RenewExpiredClientAsync(cancellationToken).ConfigureAwait(false);
        }
        else if (resultMetadata.resultStatus == ResultStatus.RENEW_PRESIGNED_URL.ToString())
        {
            await UpdatePresignedUrlAsync(cancellationToken).ConfigureAwait(false);
        }

        _resultsMetas.Add(resultMetadata);

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
        foreach (var fileMetadata in filesMetadata)
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
    /// <param name="cancellationToken">Cancellation support.</param>
    /// <returns>The result outcome for each file.</returns>
    private async Task UploadFilesInParallelAsync(List<SFFileMetadata> filesMetadata, CancellationToken cancellationToken)
    {
        foreach (var fileMetadata in filesMetadata)
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
        foreach (var fileMetadata in filesMetadata)
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
        foreach (var fileMetadata in filesMetadata)
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
    private SFFileMetadata UploadSingleFile(SFFileMetadata fileMetadata)
    {
        fileMetadata.realSrcFilePath = fileMetadata.srcFilePath;

        BorrowedMemoryStream memoryStream = null;
        try
        {
            var needsTmpDir = (fileMetadata.requireCompress && fileMetadata.canCompressSpillToFile) || fileMetadata.MaxBytesInMemory >= 0;
            if (needsTmpDir)
                fileMetadata.tmpDir = GetTemporaryDirectory();

            // Compress the file if needed
            if (fileMetadata.requireCompress)
                CompressFileWithGzip(fileMetadata, out memoryStream);

            // Calculate the digest
            GetDigestAndSizeForFile(fileMetadata);

            if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
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
            s_logger.Debug("Unhandled exception while uploading file.", ex);
            throw;
        }
        finally
        {
            if (fileMetadata.tmpDir != null)
            {
                Directory.Delete(fileMetadata.tmpDir, true);
                if (memoryStream != null)
                    fileMetadata.memoryStream = memoryStream;
            }
            else if (!fileMetadata.canCompressSpillToFile && memoryStream != null)
            {
                var compressedMemoryStream = fileMetadata.memoryStream;
                fileMetadata.memoryStream = memoryStream;
                compressedMemoryStream.Dispose();
            }
        }

        return fileMetadata;
    }

    /// <summary>
    /// Upload a single file.
    /// </summary>
    /// <param name="storageClient">Storage client to upload the file with.</param>
    /// <param name="fileMetadata">The metadata of the file to upload.</param>
    /// <param name="cancellationToken">Cancellation support.</param>
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
                CompressFileWithGzip(fileMetadata, out _);
            }

            // Calculate the digest
            GetDigestAndSizeForFile(fileMetadata);

            if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
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
            s_logger.Error("UploadSingleFileAsync encountered an error: " + ex.Message);
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
            if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
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
            s_logger.Error("DownloadSingleFile encountered an error: " + ex.Message);
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
            if (StorageClientType.REMOTE == GetStorageClientType(_transferMetadata.stageInfo))
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
            s_logger.Error("DownloadSingleFileAsync encountered an error: " + ex.Message);
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
    public static string GetTemporaryDirectory()
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempDirectory);
        return tempDirectory;
    }


    /// <summary>
    /// Get the storage client type.
    /// </summary>
    /// <param name="stageInfo">The stage info used to get the stage location type.</param>
    /// <returns>The storage client type.</returns>
    public static StorageClientType GetStorageClientType(PutGetStageInfo stageInfo)
    {
        var stageLocationType = stageInfo.locationType;

        if (stageLocationType == LOCAL_FS)
        {
            return StorageClientType.LOCAL;
        }
        else
        {
            return StorageClientType.REMOTE;
        }
    }

    private void InitFileMetadataForUpload()
    {
        // Initialize the list of actual files to upload
        var expandedSrcLocations = new List<string>();
        if (_memoryStream != null)
        {
            // stream put only support single file
            if (_transferMetadata.src_locations.Count != 1)
            {
                throw new ArgumentException("Invalid stream put.");
            }

            expandedSrcLocations.Add(_transferMetadata.src_locations[0]);
        }
        else
        {
            foreach (var location in _transferMetadata.src_locations)
            {
                expandedSrcLocations.AddRange(ExpandFileNames(location));
            }
        }

        // Initialize each file specific metadata (for example, file path, name and size) and
        // put it in1 of the 2 lists : Small files and large files based on a threshold
        // extracted from the command response
        InitFileMetadata(expandedSrcLocations);

        if (expandedSrcLocations.Count == 0)
            throw new ArgumentException("No file found for: " + _transferMetadata.src_locations[0]);
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
