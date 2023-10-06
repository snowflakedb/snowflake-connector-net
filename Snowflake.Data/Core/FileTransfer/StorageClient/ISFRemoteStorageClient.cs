/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class WrappedContentInfo
    {
        public string KeyId { get; set; }
        public string EncryptedKey { get; set; }
        public string Algorithm { get; set; }
    }

    internal class EncryptionAgentInfo
    {
        public string Protocol { get; set; }
        public string EncryptionAlgorithm { get; set; }
    }

    internal class KeyWrappingMetadataInfo
    {
        public string EncryptionLibrary { get; set; }
    }

    internal class EncryptionData
    {
        public string EncryptionMode { get; set; }
        public WrappedContentInfo WrappedContentKey { get; set; }
        public EncryptionAgentInfo EncryptionAgent { get; set; }
        public string ContentEncryptionIV { get; set; }
        public KeyWrappingMetadataInfo KeyWrappingMetadata { get; set; }
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
