/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;

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
                Directory.CreateDirectory(fileMetadata.stageInfo.location);
            }

            // Create reader stream
            Stream stream;
            if (fileMetadata.sourceFromStream && fileMetadata.memoryStream != null)
            {
                stream = fileMetadata.memoryStream;
            }
            else
            {
                stream = new MemoryStream(File.ReadAllBytes(fileMetadata.realSrcFilePath));
            }

            // Write stream to file
            using (var fileStream = File.Create(Path.Combine(fileMetadata.stageInfo.location, fileMetadata.destFileName)))
            {
                stream.CopyTo(fileStream);
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }
    }
}
