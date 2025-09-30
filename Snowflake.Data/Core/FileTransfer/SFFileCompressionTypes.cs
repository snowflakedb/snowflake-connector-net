using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class SFFileCompressionTypes
    {
        private const byte MAX_MAGIC_BYTES = 4;

        private static readonly byte[] GZIP_MAGIC = new byte[] { 0x1f, 0x8b };
        private const string GZIP_NAME = "gzip";
        private const string GZIP_EXTENSION = ".gz";

        private static readonly byte[] DEFLATE_MAGIC_LOW = new byte[] { 0x78, 0x01 };
        private static readonly byte[] DEFLATE_MAGIC_DEFAULT = new byte[] { 0x78, 0x9c };
        private static readonly byte[] DEFLATE_MAGIC_BEST = new byte[] { 0x78, 0xda };
        private const string DEFLATE_NAME = "deflate";
        private const string DEFLATE_EXTENSION = ".deflate";

        private const string RAW_DEFLATE_NAME = "raw_deflate";
        private const string RAW_DEFLATE_EXTENSION = ".raw_deflate";

        private static readonly byte[] BZIP2_MAGIC = new byte[] { 0x42, 0x5a };
        private const string BZIP2_NAME = "bzip2";
        private const string BZIP2_EXTENSION = ".bz2";

        private static readonly byte[] ZSTD_MAGIC = new byte[] { 0x28, 0xb5, 0x2f, 0xfd };
        private const string ZSTD_NAME = "zstd";
        private const string ZSTD_EXTENSION = ".zst";

        private static readonly byte[] BROTLI_MAGIC = new byte[] { 0xce, 0xb2, 0xcf, 0x81 };
        private const string BROTLI_NAME = "brotli";
        private const string BROTLI_EXTENSION = ".br";

        private const string LZIP_NAME = "lzip";
        private const string LZIP_EXTENSION = ".lz";

        private const string LZMA_NAME = "lzma";
        private const string LZMA_EXTENSION = ".lzma";

        private const string LZO_NAME = "lzop";
        private const string LZO_EXTENSION = ".lzo";

        private const string XZ_NAME = "xz";
        private const string XZ_EXTENSION = ".xz";

        private const string COMPRESS_NAME = "compress";
        private const string COMPRESS_EXTENSION = ".Z";

        private static readonly byte[] PARQUET_MAGIC = new byte[] { 0x50, 0x41, 0x52, 0x31 };
        private const string PARQUET_NAME = "parquet";
        private const string PARQUET_EXTENSION = ".parquet";

        private static readonly byte[] ORC_MAGIC = new byte[] { 0x4f, 0x52, 0x43 };
        private const string ORC_NAME = "orc";
        private const string ORC_EXTENSION = ".orc";

        private const string NONE_NAME = "none";
        private const string NONE_EXTENSION = "";

        private static readonly byte[][] gzip_magics = new[] { GZIP_MAGIC };
        private static readonly byte[][] deflate_magics =
            new[] { DEFLATE_MAGIC_LOW, DEFLATE_MAGIC_DEFAULT, DEFLATE_MAGIC_BEST };
        private static readonly byte[][] bzip2_magics = new[] { BZIP2_MAGIC };
        private static readonly byte[][] orc_magics = new[] { ORC_MAGIC };
        private static readonly byte[][] parquet_magics = new[] { PARQUET_MAGIC };
        private static readonly byte[][] zstd_magics = new[] { ZSTD_MAGIC };
        private static readonly byte[][] brotli_magics = new[] { BROTLI_MAGIC };

        public struct SFFileCompressionType
        {
            public SFFileCompressionType(
                string fileExtension,
                string name,
                byte[][] magicNumbers,
                short magicBytes,
                bool isSupported)
            {
                FileExtension = fileExtension;
                IsSupported = isSupported;
                _magicNumbers = magicNumbers;
                _magicBytes = magicBytes;
                Name = name;
            }

            public SFFileCompressionType(
                string fileExtension,
                string name,
                bool isSupported)
            {
                FileExtension = fileExtension;
                IsSupported = isSupported;
                _magicNumbers = null;
                _magicBytes = 0;
                Name = name;
            }

            /// <summary>
            /// Check if the given header matches the magic number for this compression type
            /// </summary>
            /// <param name="header"></param>
            /// <returns></returns>
            public bool matchMagicNumber(byte[] header)
            {
                if (_magicNumbers != null && _magicNumbers.Length > 0)
                    foreach (byte[] m in _magicNumbers)
                        if (m != null && header != null && m.Length > 0 && header.Length > 0)
                            if (new ReadOnlySpan<byte>(m).SequenceEqual(new ReadOnlySpan<byte>(header, 0, m.Length)))
                                return true;

                return false;
            }

            internal string FileExtension { get; }
            internal string Name { get; }
            private readonly byte[][] _magicNumbers;
            private readonly short _magicBytes;
            internal bool IsSupported { get; }
        }

        public static readonly SFFileCompressionType GZIP =
            new SFFileCompressionType(GZIP_EXTENSION, GZIP_NAME, gzip_magics, 2, true);

        public static readonly SFFileCompressionType DEFLATE =
            new SFFileCompressionType(DEFLATE_EXTENSION, DEFLATE_NAME, deflate_magics, 2, true);

        public static readonly SFFileCompressionType RAW_DEFLATE =
            new SFFileCompressionType(RAW_DEFLATE_EXTENSION, RAW_DEFLATE_NAME, true);

        public static readonly SFFileCompressionType BZIP2 =
            new SFFileCompressionType(BZIP2_EXTENSION, BZIP2_NAME, bzip2_magics, 2, true);

        public static readonly SFFileCompressionType ZSTD =
            new SFFileCompressionType(ZSTD_EXTENSION, ZSTD_NAME, zstd_magics, 4, true);

        public static readonly SFFileCompressionType BROTLI =
            new SFFileCompressionType(BROTLI_EXTENSION, BROTLI_NAME, brotli_magics, 4, true);

        public static readonly SFFileCompressionType ORC =
            new SFFileCompressionType(ORC_EXTENSION, ORC_NAME, orc_magics, 3, true);

        public static readonly SFFileCompressionType PARQUET =
            new SFFileCompressionType(PARQUET_EXTENSION, PARQUET_NAME, parquet_magics, 4, true);

        public static readonly SFFileCompressionType LZIP =
            new SFFileCompressionType(LZIP_EXTENSION, LZIP_NAME, false);

        public static readonly SFFileCompressionType LZMA =
            new SFFileCompressionType(LZMA_EXTENSION, LZMA_NAME, false);

        public static readonly SFFileCompressionType LZO =
            new SFFileCompressionType(LZO_EXTENSION, LZO_NAME, false);

        public static readonly SFFileCompressionType XZ =
            new SFFileCompressionType(XZ_EXTENSION, XZ_NAME, false);

        public static readonly SFFileCompressionType COMPRESS =
            new SFFileCompressionType(COMPRESS_EXTENSION, COMPRESS_NAME, false);

        public static readonly SFFileCompressionType NONE =
            new SFFileCompressionType(NONE_EXTENSION, NONE_NAME, true);


        static readonly IReadOnlyList<SFFileCompressionType> compressionTypes =
            new List<SFFileCompressionType> {
                GZIP,
                DEFLATE,
                RAW_DEFLATE,
                BZIP2,
                ZSTD,
                BROTLI,
                LZIP,
                LZMA,
                LZO,
                XZ,
                COMPRESS,
                ORC,
                PARQUET
            };

        public static SFFileCompressionType GuessCompressionType(string filePath)
        {
            // read first 4 bytes to determine compression type
            byte[] header = new byte[MAX_MAGIC_BYTES];
            using (FileStream fs = File.OpenRead(filePath))
            {
                fs.Read(header, 0, header.Length);
            }

            foreach (SFFileCompressionType compType in compressionTypes)
            {
                if (compType.matchMagicNumber(header))
                {
                    // Found the compression type for this file
                    string extension = Path.GetExtension(filePath);
                    if (!String.IsNullOrEmpty(extension) &&
                        String.Equals(BROTLI.FileExtension, extension, StringComparison.OrdinalIgnoreCase))
                    {
                        return BROTLI;
                    }
                    else
                    {
                        return compType;
                    }
                }
            }

            // Couldn't find a match, last fallback using the file name extension
            return LookUpByFileExtension(new FileInfo(filePath).Extension);
        }

        public static SFFileCompressionType LookUpByFileExtension(string fileExtension)
        {
            if (!fileExtension.StartsWith("."))
            {
                fileExtension = "." + fileExtension;
            }
            foreach (SFFileCompressionType compType in compressionTypes)
            {
                if (compType.FileExtension.Equals(fileExtension, StringComparison.InvariantCultureIgnoreCase))
                {
                    return compType;
                }
            }
            return NONE;
        }

        /// <summary>
        /// Lookup the compression type base on the given type name.
        /// </summary>
        /// <param name="name">The type name to lookup</param>
        /// <returns>The corresponding SFFileCompressionType if supported, None if no match</returns>
        public static SFFileCompressionType LookUpByName(string name)
        {
            if (name.StartsWith("."))
            {
                name = name.Substring(1);
            }
            foreach (SFFileCompressionType compType in compressionTypes)
            {
                if (compType.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase))
                {
                    return compType;
                }
            }

            return NONE;
        }
    }
}
