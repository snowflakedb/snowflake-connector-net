using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class WrappedContentInfo
    {
        public string KeyId;
        public string EncryptedKey;
        public string Algorithm;
    }

    internal class EncryptionAgentInfo
    {
        public string Protocol;
        public string EncryptionAlgorithm;
    }

    internal class KeyWrappingMetadataInfo
    {
        public string EncryptionLibrary;
    }

    internal class EncryptionData
    {
        public string EncryptionMode;
        public WrappedContentInfo WrappedContentKey;
        public EncryptionAgentInfo EncryptionAgent;
        public string ContentEncryptionIV;
        public KeyWrappingMetadataInfo KeyWrappingMetadata;
    }

    internal class RemoteLocation
    {
        public string bucket { get; set; }
        public string key { get; set; }
    }

    /// <summary>
    /// The interface for the storage clients. 
    /// </summary>
    interface ISFRemoteStorageClient
    {
        /// <summary>
        /// Get the bucket name and path.
        /// </summary>
        RemoteLocation ExtractBucketNameAndPath(string stageLocation);

        /// <summary>
        /// Encrypt then upload one file.
        /// </summary>
        FileHeader GetFileHeader(SFFileMetadata fileMetadata);

        /// <summary>
        /// Encrypt then upload one file.
        /// </summary>
        Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken);

        /// <summary>
        /// Attempt upload of a file and retry if fails.
        /// </summary>
        void UploadFile(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata);

        /// <summary>
        /// Attempt upload of a file and retry if fails.
        /// </summary>
        Task UploadFileAsync(SFFileMetadata fileMetadata, Stream fileBytesStream, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken);

        /// <summary>
        /// Attempt download of a file and retry if fails.
        /// </summary>
        void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency);

        /// <summary>
        /// Attempt download of a file and retry if fails.
        /// </summary>
        Task DownloadFileAsync(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency, CancellationToken cancellationToken);
    }
}
