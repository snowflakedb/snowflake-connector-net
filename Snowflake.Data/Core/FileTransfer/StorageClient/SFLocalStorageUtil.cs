using System.IO;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.FileTransfer
{
    /// <summary>
    /// The storage client for local upload/download.
    /// </summary>
    class SFLocalStorageUtil
    {
        /// <summary>
        /// Write the file locally.
        /// <param name="fileMetadata">The metadata of the file to upload.</param>
        /// </summary>
        internal static void UploadOneFileWithRetry(SFFileMetadata fileMetadata)
        {
            // Create directory if doesn't exist
            if (!Directory.Exists(fileMetadata.stageInfo.location))
            {
                DirectoryOperations.Instance.CreateDirectory(fileMetadata.stageInfo.location);
            }

            // Create reader stream
            Stream stream;
            if (fileMetadata.memoryStream != null)
            {
                stream = fileMetadata.memoryStream;
            }
            else
            {
                stream = new MemoryStream(File.ReadAllBytes(fileMetadata.realSrcFilePath));
            }
            stream.Position = 0;

            // Write stream to file
            using (var fileStream = FileOperations.Instance.Create(Path.Combine(fileMetadata.stageInfo.location, fileMetadata.destFileName)))
            {
                stream.CopyTo(fileStream);
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Download the file locally.
        /// <param name="fileMetadata">The metadata of the file to download.</param>
        /// </summary>
        internal static void DownloadOneFile(SFFileMetadata fileMetadata)
        {
            string srcFilePath = fileMetadata.stageInfo.location;
            string realSrcFilePath = Path.Combine(srcFilePath, fileMetadata.srcFileName);
            string output = Path.Combine(fileMetadata.localLocation, fileMetadata.destFileName);

            // Create directory if doesn't exist
            if (!Directory.Exists(fileMetadata.localLocation))
            {
                DirectoryOperations.Instance.CreateDirectory(fileMetadata.localLocation);
            }

            // Create stream object for reader and writer
            Stream stream = new MemoryStream(File.ReadAllBytes(realSrcFilePath));
            using (var fileStream = FileOperations.Instance.Create(output))
            {
                // Write file
                stream.CopyTo(fileStream);
                fileMetadata.destFileSize = fileStream.Length;
            }

            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }
    }
}
