/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core.FileTransfert;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Threading;

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
        SKIP_UPLOAD_FILE
    }

    internal enum Command
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
        /// The Snowflake session
        /// </summary>
        private SFSession Session;

        /// <summary>
        /// External cancellation token, used to stop the transfer
        /// </summary>
        private CancellationToken externalCancellationToken;

        /// The storage client used to upload/download data from files or streams
        private ISFStorageClient StorageClient;

        /// <summary>
        /// The status for each file transfer.
        /// </summary>
        private List<FileTransferOutcome> Status;

        /// -------------- Command metadata - Applies to all files being transfered ----------- ///
        
        /// <summary>
        /// The command type.
        /// </summary>
        private Command CommandType;

        /// <summary>
        /// The sql command.
        /// </summary>
        private String Command;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SFFileTransferAgent(
            SFSession session, 
            String command,
            QueryExecResponseData responseData, 
            CancellationToken cancellationToken)
        {
            Session = session;
            Command = command;

            // Parse the result to initialize the transfer metadata
        }

        /// <summary>
        /// Execute the transfer command.
        /// </summary>
        public List<FileTransferOutcome> execute()
        {
            List<FileTransferOutcome> results = null;

            return results;
        }

        private List<FileTransferOutcome> executeUpload()
        {
            List<FileTransferOutcome> results = null;
            // Initialize the list of actual files to upload
            // Initialize each file specific metadata (for example, file path, name and size) and 
            // put it in1 of the 2 lists : Small files and large files based on a threshold 
            // extracted from the command response

            //  Retrieve / Compute the file actual compression type for each file in the list(most work is for auto - detect)

            //Start the upload tasks(for small files upload in parallel using the given parallelism factor, for large file updload sequentially)
            //For each file, using the remote client


            // Report the transfert status for each file
            return results;
        }


        /// <summary>
        /// Compress a file using the given file metadata (file path, compression type, etc...) and
        /// update the metadata accordingly after the compression is finished.
        /// </summary>
        /// <param name="fileMetadata">The metadata for the file to compress.</param>
        private void compressFile(SFFileMetadata fileMetadata)
        {

        }

        /// <summary>
        /// Renew the storage client expired token. It retrieves a fresh token from GS and then 
        /// update the storage client.
        /// </summary>
        private void renewExpiredToken()
        {

        }

        /// <summary>
        /// Upload a list of files in parallel;
        /// </summary>
        /// <returns>The result outcome for each file.</returns>
        private List<FileTransferOutcome> uploadFilesInParallel()
        {
            List<FileTransferOutcome> results = null;

            return results;
        }

        /// <summary>
        /// Upload a single file.
        /// </summary>
        /// <returns>The result outcome.</returns>
        private FileTransferOutcome uploadSingleFile(
            ISFStorageClient storageClient,
            SFFileMetadata fileMetadata)
        {
            FileTransferOutcome result = FileTransferOutcome.FAILED;

            // Verify that the file doesn't exist already unless overwrite is true
            //Update the file metadata with presigned urls if any(only available for GCS for now)
            // Compress the file if needed
            // Calculate the digest
            // Initialize the encryption metadata and encrypt the file if needed
            // Upload the file using the remote client SDK and the file metadata

            return result;
        }

    }
}
