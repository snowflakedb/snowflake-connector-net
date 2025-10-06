using System;
using System.IO;
using static Snowflake.Data.Core.FileTransfer.SFFileCompressionTypes;

namespace Snowflake.Data.Core.FileTransfer
{
    public class SFEncryptionMetadata
    {
        /// Initialization vector for file content encryption
        public string iv { set; get; }

        /// File key
        public string key { set; get; }

        /// Additional Authentication Data for file content encryption
        public string aad { set; get; }

        /// Initialization vector for key encryption
        public string keyIV { set; get; }

        /// Additional Authentication Data for key encryption
        public string keyAad { set; get; }

        /// Encryption material descriptor
        public string matDesc { set; get; }
    }

    /// <summary>
    /// The proxy credentials of the client.
    /// </summary>
    internal class ProxyCredentials
    {
        public string ProxyHost { get; set; }
        public int ProxyPort { get; set; }
        public string ProxyUser { get; set; }
        public string ProxyPassword { get; set; }
    }

    /// <summary>
    /// Metadata used by the remote storage client to upload or download a file/stream.
    /// </summary>
    internal class SFFileMetadata
    {
        /// Original source file path (full path)
        public string srcFilePath { set; get; }

        /// Original path or temp path when compression is enabled (full path)
        public string realSrcFilePath { set; get; }

        /// Original source file name
        public string srcFileName { set; get; }

        /// Original source file size
        public long srcFileSize { set; get; }

        /// Temp file if compressed is required, otherwise same as src file
        public string srcFileToUpload { set; get; }

        /// Temp file size if compressed is required, otherwise same as src file
        public long srcFileToUploadSize { set; get; }

        /// Destination file name (no path)
        public string destFileName { set; get; }

        /// Destination file size
        public long destFileSize { set; get; }

        /// Absolute path to the destination (including the filename. /tmp/small_test_file.csv.gz)
        public string destPath { set; get; }

        /// Absolute path to the local location of the downloaded file
        public string localLocation { set; get; }

        /// Destination file size
        public long uploadSize { set; get; }

        /// Stage info of the file
        public PutGetStageInfo stageInfo { get; set; }

        /// True if require gzip compression
        public bool requireCompress { set; get; }

        /// Upload and overwrite if file exists
        public bool overwrite { set; get; }

        /// Encryption material
        public PutGetEncryptionMaterial encryptionMaterial { set; get; }

        /// Encryption metadata
        public SFEncryptionMetadata encryptionMetadata { set; get; }

        /// File message digest (after compression if required)
        public string sha256Digest { set; get; }

        /// Source compression
        public SFFileCompressionType sourceCompression { set; get; }

        /// Target compression
        public SFFileCompressionType targetCompression { set; get; }

        /// Pre-signed url.
        public string presignedUrl { set; get; }

        /// The number of chunks to download in parallel.
        public int parallel { get; set; }

        /// The outcome of the transfer.
        public string resultStatus { get; set; }

        /// The temporary directory to store files to upload/download.
        public string tmpDir { get; set; }

        /// Storage client to use for uploading/downloading files.
        public ISFRemoteStorageClient client { get; set; }

        /// Last error returned from client request.
        public Exception lastError { get; set; }

        /// Last specified max concurrency to use.
        public int lastMaxConcurrency { get; set; }

        public bool sourceFromStream { get; set; }

        public MemoryStream memoryStream { get; set; }

        // Proxy credentials of the remote storage client.
        public ProxyCredentials proxyCredentials { get; set; }

        public int MaxBytesInMemory { get; set; }

        internal CommandTypes _operationType;

        internal string RemoteFileName()
        {
            if (_operationType == CommandTypes.UPLOAD)
            {
                return destFileName;
            }

            return srcFileName;
        }
    }

    internal class FileTransferConfiguration
    {

        private const int OneMegabyteInBytes = 1024 * 1024;

        public string TempDir { get; set; }

        public int MaxBytesInMemory { get; set; }

        public static FileTransferConfiguration FromFileMetadata(SFFileMetadata fileMetadata) =>
            new FileTransferConfiguration()
            {
                TempDir = fileMetadata.tmpDir ?? DefaultTempDir,
                MaxBytesInMemory = fileMetadata.MaxBytesInMemory
            };

        public static int DefaultMaxBytesInMemory => OneMegabyteInBytes;

        private static string DefaultTempDir => Path.GetTempPath();
    }
}
