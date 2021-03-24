using System;
using System.Collections.Generic;
using System.Text;
using static Snowflake.Data.Core.FileTransfert.SFFileCompressionTypes;

namespace Snowflake.Data.Core.FileTransfert
{
    /// <summary>
    /// Metadata used by the remote storage client to upload or download a file/stream.
    /// </summary>
    internal class SFFileMetadata
    {
        public class SFEncryptionMetadata
        {
            /// Initialization vector
            public string iv { set; get; }

            /// File key
            public string fileKey { set; get; }

            /// Base 64 encoded of encrypted file key
            public string enKekEncoded { set; get; }

            /// Encryption material descriptor
            public string matDesc { set; get; }

            /// Encrypted stream size, used for content length
            public long cipherStreamSize { set; get; }
        }

        /// Original source file name (full path)
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

        /// True if require gzip compression
        public bool requireCompress { set; get; }

        /// Upload and overwrite if file exists
        public bool overWrite { set; get; }

        /// Encryption metadata
        public SFEncryptionMetadata encryptionMetadata { set; get; }

        /// File message digest (after compression if required)
        public string sha256Digest { set; get; }

        /// Source compression 
        public SFFileCompressionType sourceCompression { set; get; }

        /// Target compression
        public SFFileCompressionType targetCompression { set; get; }

        /// Pre-signed url
        public string presignedUrl { set; get; }

    }
}
