/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Client;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// The possible status for one file transfer
    /// </summary>
    enum FileTransferOutcome
    {
        SUCCESS,
        FAILED,
        TOKEN_EXPIRED,
        SKIP_UPLOAD_FILE,
        UNKNOWN
    }

    internal enum CommandTypes
    {
        UPLOAD,
        DOWNLOAD
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
        /// none keyword for no source compression.
        /// </summary>
        private static readonly string COMPRESSION_NONE = "none";

        /// <summary>
        /// The Snowflake session
        /// </summary>
        private SFSession Session;

        /// <summary>
        /// External cancellation token, used to stop the transfer
        /// </summary>
        private CancellationToken externalCancellationToken;

        /// <summary>
        /// The status for each file transfer.
        /// </summary>
        private List<FileTransferOutcome> Status;

        /// <summary>
        /// The type of transfer either UPLOAD or DOWNLOAD.
        /// </summary>
        private readonly CommandTypes CommandType;

        /// <summary>
        /// The transfer metadata. Applies to all files being transfered
        /// </summary>
        private readonly PutGetResponseData TransferMetadata;

        /// <summary>
        /// The path to the user home directory.
        /// </summary>
        private readonly string HomePath = (
            Environment.OSVersion.Platform == PlatformID.Unix ||
            Environment.OSVersion.Platform == PlatformID.MacOSX) ?
              Environment.GetEnvironmentVariable("HOME") :
              Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

        private List<SFFileMetadata>  SmallFilesMetas = new List<SFFileMetadata>();
        private List<SFFileMetadata>  LargeFilesMetas = new List<SFFileMetadata>();

        /// <summary>
        /// Constructor.
        /// </summary>
        public SFFileTransferAgent(
            SFSession session, 
            PutGetResponseData responseData, 
            CancellationToken cancellationToken)
        {
            Session = session;
            TransferMetadata = responseData;
            CommandType = (CommandTypes)Enum.Parse(typeof(CommandTypes), TransferMetadata.command, true);
            externalCancellationToken = cancellationToken;
        }

        /// <summary>
        /// Execute the transfer command.
        /// </summary>
        public List<FileTransferOutcome> execute()
        {
            List<FileTransferOutcome> results = null;

            if (CommandTypes.UPLOAD == CommandType)
            {
                executeUpload();
            }

            return results;
        }

        private List<FileTransferOutcome> executeUpload()
        {
            List<FileTransferOutcome> results = null;
            // Initialize the list of actual files to upload
            List<string> expandedSrcLocations = new List<string>(); ;
            foreach (string location in TransferMetadata.src_locations)
            {
                expandedSrcLocations.AddRange(expandFileNames(location));
            }

            // Initialize each file specific metadata (for example, file path, name and size) and 
            // put it in1 of the 2 lists : Small files and large files based on a threshold 
            // extracted from the command response
            initFileMetadata(expandedSrcLocations);

            //Start the upload tasks(for small files upload in parallel using the given parallelism 
            //factor, for large file updload sequentially)
            //For each file, using the remote client
            if (0 < LargeFilesMetas.Count)
            {
                Logger.Debug("Start uploading large files");            
                ISFStorageClient storageClient =
                    SFStorageClientFactory.newStorageClient(TransferMetadata.stageInfo);
                foreach (SFFileMetadata fileMeta in LargeFilesMetas)
                {
                    Status.Add(UploadSingleFile(storageClient, fileMeta));
                }

                Logger.Debug("End uploading large files");
            }

            if (0 < SmallFilesMetas.Count)
            {
                Logger.Debug("Start uploading small files");
                UploadFilesInParallel(SmallFilesMetas, TransferMetadata.parallel);
                Logger.Debug("End uploading small files");
            }


            // Report the transfer status for each file
            return results;
        }

        private void initFileMetadata(
            List<string> files)
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

                SFFileMetadata fileMeta = new SFFileMetadata()
                {
                    srcFilePath = file,
                    srcFileName = fileName,
                    srcFileSize = fileInfo.Length,
                    overWrite = TransferMetadata.overwrite,
                    // Need to compress before sending only if autoCompress is On and the file is 
                    // not compressed yet
                    requireCompress = (
                        TransferMetadata.autoCompress && 
                        (SFFileCompressionTypes.NONE.Equals(compressionType))),
                    sourceCompression = compressionType,
                    presignedUrl = TransferMetadata.stageInfo.presignedUrl,
                    // If the file is under the threshold, don't upload in chunks, set parallel to 1
                    parallel = (fileInfo.Length > TransferMetadata.threshold) ? 
                        TransferMetadata.parallel : 1,
                    transferOutcome = FileTransferOutcome.UNKNOWN
                };

                
                if (!fileMeta.requireCompress)
                {
                    // The file is already compressed
                    fileMeta.targetCompression = fileMeta.sourceCompression;
                    fileMeta.destFileName = fileName;
                }
                else
                {
                    // The file will need to be compressed using gzip
                    fileMeta.targetCompression = SFFileCompressionTypes.GZIP;
                    fileMeta.destFileName = fileName  + SFFileCompressionTypes.GZIP.FileExtension;
                }

                // If the file is larger than the threshold, add it to the large files list
                // Otherwise add it to the small files list
                if (fileMeta.srcFileSize > TransferMetadata.threshold)
                {
                    LargeFilesMetas.Add(fileMeta);
                }
                else
                {
                    SmallFilesMetas.Add(fileMeta);
                }
            }
        }

        /// <summary>
        /// Expand the expand the wildcards if any to generate the list of paths for all files 
        /// matched by the wildcards. Also replace 
        /// Get the absolute path for the file.
        /// </summary>
        /// <param name="location">The path to expand</param>
        /// <returns>The list of file matching the input location</returns>
        /// <exception cref="DirectoryNotFoundException">Directory not found. Could not find a part of the pat </exception>
        /// <exception cref="FileNotFoundException">File not found or the path is pointing to a Directory</exception>
        private List<string> expandFileNames(string location)
        {
            // Replace ~ with the user home directory path
            if (location.Contains("~"))
            {
                string homePath = (Environment.OSVersion.Platform == PlatformID.Unix ||
                Environment.OSVersion.Platform == PlatformID.MacOSX)
                ? Environment.GetEnvironmentVariable("HOME")
                : Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

                location = location.Replace("~", homePath);
            }

            location = Path.GetFullPath(location);
            String fileName = Path.GetFileName(location);
            string directoryName = Path.GetDirectoryName(location);

            List<string> filePaths = new List<string>();
            filePaths.Add(""); //Start with an empty string to build upon
            if (directoryName.Contains("?") ||
                directoryName.Contains("*"))
            {
                // If there is a wildcard in at least one of the directory name in the file path
                string[] pathParts = location.Split(Path.DirectorySeparatorChar);

                string currPart;
                for (int i = 0; i < pathParts.Length; i++)
                {
                    List<string> tempPaths = new List<string>();
                    foreach (string filePath in filePaths)
                    {
                        currPart = pathParts[i];

                        if (currPart.Contains("?") || currPart.Contains("*"))
                        {
                            if (i < pathParts.Length - 1)
                            {
                                // Expand the directories names
                                tempPaths.AddRange(
                                    Directory.GetDirectories(
                                        filePath,
                                        currPart,
                                        SearchOption.TopDirectoryOnly));
                            }
                            else
                            {
                                // Expand the files names
                                tempPaths.AddRange(
                                    Directory.GetFiles(
                                        filePath, 
                                        currPart, 
                                        SearchOption.TopDirectoryOnly));
                            }
                        }
                        else
                        {
                            if (0 < i)
                            {
                                // Keep building the paths
                                tempPaths.Add(filePath + Path.DirectorySeparatorChar + currPart);
                            }
                            else
                            {
                                // First part
                                tempPaths.Add(currPart);
                            }
                        }
                    }
                    filePaths = tempPaths;
                }
            }
            else if (fileName.Contains("?") || fileName.Contains("*"))
            {
                string ext = Path.GetExtension(fileName);
                if ((4 == ext.Length) && (fileName.Contains("*")))
                {
                    /*
                        * When you use the asterisk wildcard character in a searchPattern such as
                        * "*.txt", the number of characters in the specified extension affects the
                        * search as follows:
                        * - If the specified extension is exactly three characters long, the method
                        * returns files with extensions that begin with the specified extension. 
                        * For example, "*.xls" returns both "book.xls" and "book.xlsx".
                        * - In all other cases, the method returns files that exactly match the 
                        * specified extension. For example, "*.ai" returns "file.ai" but not "file.aif".
                        */
                string[] potentialMatches = 
                        Directory.GetFiles(
                            directoryName, 
                            fileName, 
                            SearchOption.TopDirectoryOnly);
                    foreach (string potentialMatch in potentialMatches)
                    {
                        if (potentialMatch.EndsWith(ext))
                        {
                            filePaths.Add(potentialMatch);
                        }
                    }
                }
                else
                {
                    // If there is a wildcard in the file name in the file path
                    filePaths.AddRange(
                        Directory.GetFiles(
                            directoryName, 
                            fileName, 
                            SearchOption.TopDirectoryOnly));
                }
            }
            else
            {
                // No wild card, just make sure it's a file
                FileAttributes attr = File.GetAttributes(location);
                if (!attr.HasFlag(FileAttributes.Directory))
                {
                    filePaths.Add(location);
                }
                else
                {                
                    throw new FileNotFoundException(
                        "Directories not supported, you need to provide a file path", location);
                }
            }

            if (Logger.IsDebugEnabled())
            {
                Logger.Debug("Expand " + location + " into : \n");
                foreach (string filepath in filePaths)
                {
                    Logger.Debug("\t" + filepath + "\n");
                }
            }

            return filePaths;
        }


        /// <summary>
        /// Compress a file using the given file metadata (file path, compression type, etc...) and
        /// update the metadata accordingly after the compression is finished.
        /// </summary>
        /// <param name="fileMetadata">The metadata for the file to compress.</param>
        private void compressFile(SFFileMetadata fileMetadata)
        {
            FileInfo fileToCompress = new FileInfo(fileMetadata.srcFilePath);
            using (FileStream originalFileStream = fileToCompress.OpenRead())
            {
                if ((File.GetAttributes(fileToCompress.FullName) &
                   FileAttributes.Hidden) != FileAttributes.Hidden)
                {
                    using (FileStream compressedFileStream = File.Create(fileMetadata.destFileName))
                    {
                        using (GZipStream compressionStream = 
                            new GZipStream(compressedFileStream, CompressionMode.Compress))
                        {
                            originalFileStream.CopyTo(compressionStream);
                        }
                    }

                    Logger.Debug($"Compressed {fileToCompress.Name} to {fileMetadata.destFileName}");
                    FileInfo destInfo = new FileInfo(fileMetadata.destFileName);
                    fileMetadata.destFileSize = destInfo.Length;
                }
            }
        }

        /// <summary>
        /// Renew the storage client expired token. It retrieves a fresh token from GS and then 
        /// update the storage client.
        /// </summary>
        private void renewExpiredToken()
        {

        }

        /// <summary>
        /// Upload a list of files in parallel using the given parallelization factor.
        /// </summary>
        /// <param name="filesMetadata">The list of files to upload in parallel.</param>
        /// <param name="parallel">The number of files to upload in parallel.</param>
        /// <returns>The result outcome for each file.</returns>
        private List<FileTransferOutcome> UploadFilesInParallel(
            List<SFFileMetadata> filesMetadata,
            int parallel)
        {
            List<FileTransferOutcome> results = null;

            var listOfActions = new List<Action>();
            foreach (SFFileMetadata fileMeta in filesMetadata)
            {
                /// The storage client used to upload/download data from files or streams
                ISFStorageClient storageClient =
                    SFStorageClientFactory.newStorageClient(TransferMetadata.stageInfo);
                listOfActions.Add(() => Status.Add(UploadSingleFile(storageClient, fileMeta)));
            }

            var options = new ParallelOptions { MaxDegreeOfParallelism = parallel };
            Parallel.Invoke(options, listOfActions.ToArray());

            return results;
        }

        /// <summary>
        /// Upload a single file.
        /// </summary>
        /// <returns>The result outcome.</returns>
        private FileTransferOutcome UploadSingleFile(
            ISFStorageClient storageClient,
            SFFileMetadata fileMetadata)
        {
            FileTransferOutcome result = FileTransferOutcome.FAILED;

            // Verify that the file doesn't exist already unless overwrite is true
            // Update the file metadata with presigned urls if any(only available for GCS for now)
            // Compress the file if needed
            // Calculate the digest
            // Initialize the encryption metadata and encrypt the file if needed
            // Upload the file using the remote client SDK and the file metadata

            return result;
        }
    }
}
