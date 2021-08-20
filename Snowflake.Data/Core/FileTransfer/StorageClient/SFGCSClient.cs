/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The GCS client used to transfer files to the remote Google Cloud Storage.
    /// </summary>
    class SFGCSClient
    {
        /// <summary>
        /// The attribute in the credential map containing the access token.
        /// </summary>
        private static readonly string GCS_ACCESS_TOKEN = "GCS_ACCESS_TOKEN";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFGCSClient>();

        /// <summary>
        /// The storage client.
        /// </summary>
        private Google.Cloud.Storage.V1.StorageClient StorageClient;

        /// <summary>
        /// An AES-256 key passed to the Google Cloud Storage servers via headers to allow objects
        //  to be encrypted at rest using a client-supplied key.
        /// </summary>
        private EncryptionKey EncryptionKey;

        private UploadObjectOptions UploadObjectOptions;

        public SFGCSClient(PutGetStageinfo stageInfo, PutGetEncryptionMaterial encryptionMaterial)
        {
            Logger.Debug("Setting up a new GCS client ");

            if (stageInfo.isClientSideEncrypted)
            {
                byte[] decodedMasterKey = 
                    Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
                int masterKeySize = decodedMasterKey.Length * 8;

                Logger.Debug($"Master key size : {masterKeySize}");

                Random rnd = new Random();
                Byte[] encryptionKeyByte = new Byte[32];
                rnd.NextBytes(encryptionKeyByte);

                //The raw key data; must be non-null and 32 bytes long.
                EncryptionKey = EncryptionKey.Create(encryptionKeyByte);
                UploadObjectOptions = new UploadObjectOptions { EncryptionKey = EncryptionKey };
            }

            if (stageInfo.stageCredentials.TryGetValue(GCS_ACCESS_TOKEN, out string accessToken))
            {
                Logger.Debug("Constructing client using access token");

                GoogleCredential creds = GoogleCredential.FromAccessToken(accessToken, null);
                StorageClient = Google.Cloud.Storage.V1.StorageClient.Create(creds, EncryptionKey);
            }
            else
            {
                Logger.Info("No access token received from GS, constructing anonymous client with no encryption support");
                StorageClient = Google.Cloud.Storage.V1.StorageClient.CreateUnauthenticated();
            }
        }

        private void UploadFile(string bucketName, string source, string objectName)
        {
            using (FileStream fileStream = File.OpenRead(source))
            {
                // Doesn't work on anonymous client
                StorageClient.UploadObject(bucketName, objectName, null, fileStream, UploadObjectOptions);
            }
        }


        /*
        private uploadFileWithPresignedUrl(string presignedUrl, string source, string encoding)
        {
            ByteArrayContent content;
            using (FileStream stream = File.OpenRead(source))
            {
                byte[] data = new byte[stream.Length];
                stream.Read(data, 0, data.Length);
                content = new ByteArrayContent(data)
                {
                    Headers = { ContentType = new MediaTypeHeaderValue(encoding) }
                };
            }

            HttpResponseMessage response = //await
            httpClient.PutAsync(url, content);

        }
    */
    }
}
