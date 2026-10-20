using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Authentication;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests
{
    /// <summary>
    /// Covers the paths where a requested TLS floor cannot reach the socket. Asserting that a handler
    /// carries SslProtocols is not enough on its own: it says nothing about transports that never get
    /// the setting at all, which is where the enforcement was previously lost.
    /// </summary>
    public class StorageTlsEnforcementTest
    {
        private static PutGetStageInfo GcsStageInfo() =>
            new PutGetStageInfo
            {
                locationType = SFRemoteStorageUtil.GCS_FS,
                location = "test-bucket/path",
                path = "path",
                stageCredentials = new Dictionary<string, string> { { "GCS_ACCESS_TOKEN", "token" } }
            };

        [SFTheory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestGcsTransfersProceedRegardlessOfRequestedProtocols(bool tlsProtocolsExplicitlyRequested)
        {
            // arrange - GCS cannot apply a requested floor on either path, and the driver reports that
            // rather than failing the transfer, so a client must build and serve requests either way
            var client = new SFGCSClient(GcsStageInfo(), SslProtocols.Tls12 | SslProtocolsExtensions.Tls13,
                tlsProtocolsExplicitlyRequested);
            var fileMetadata = new SFFileMetadata { stageInfo = GcsStageInfo(), destFileName = "file.txt" };

            // act
            var request = client.FormBaseRequest(fileMetadata, "PUT");

            // assert
            Assert.NotNull(request);
            Assert.Equal("PUT", request.Method);
        }

        [SFTheory]
        [InlineData(SFRemoteStorageUtil.GCS_FS)]
        [InlineData(SFRemoteStorageUtil.AZURE_FS)]
        public void TestStorageClientIsBuiltWithoutRequestingProtocols(string locationType)
        {
            // arrange - the effective protocols always carry the MINTLS/MAXTLS defaults, so a client
            // built without an explicit request must still come up with the SDK stack untouched
            var response = new PutGetResponseData
            {
                stageInfo = new PutGetStageInfo
                {
                    locationType = locationType,
                    location = "test-bucket/path",
                    path = "path",
                    storageAccount = "account",
                    endPoint = "blob.core.windows.net",
                    stageCredentials = new Dictionary<string, string>
                    {
                        { "GCS_ACCESS_TOKEN", "token" }, { "AZURE_SAS_TOKEN", "?sig=token" }
                    }
                },
                parallel = 1
            };

            // act
            var client = SFRemoteStorageUtil.GetRemoteStorage(response,
                tlsProtocols: SslProtocols.Tls12 | SslProtocolsExtensions.Tls13);

            // assert
            Assert.NotNull(client);
        }

        [SFTheory]
        [InlineData("account=test;user=test;password=test", SslProtocols.Tls12 | SslProtocolsExtensions.Tls13, false)]
        [InlineData("account=test;user=test;password=test;minTls=tls13", SslProtocolsExtensions.Tls13, true)]
        public void TestSessionHandsTlsProtocolsToStorageClients(string connectionString, SslProtocols expectedProtocols,
            bool expectedExplicitlyRequested)
        {
            // arrange - an explicit request fails the connection on runtimes that cannot apply it
            Skip.When(expectedExplicitlyRequested && !CanRuntimeApplyTlsProtocols(), TlsProtocolsUnsupportedRationale);

            // act - storage clients read the protocols from the session, not from the connection's HttpClientConfig
            var session = new SFSession(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedProtocols, session.TlsProtocols);
            Assert.Equal(expectedExplicitlyRequested, session.TlsProtocolsExplicitlyRequested);
        }

        private const string TlsProtocolsUnsupportedRationale =
            "This runtime does not support setting TLS protocols on HttpClientHandler (.NET Framework 4.6.2 and 4.7.1).";

        private static bool CanRuntimeApplyTlsProtocols()
        {
            try
            {
                using var handler = new HttpClientHandler();
                handler.SslProtocols = SslProtocols.Tls12 | SslProtocolsExtensions.Tls13;
                return true;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
        }
    }
}
