using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.Collections.Generic;
    using System.Threading;

    [TestFixture, NonParallelizable]
    class ChunkDownloaderFactoryTest
    {
        int ChunkDownloaderVersionDefault = SFConfiguration.Instance().GetChunkDownloaderVersion();

        [TearDown]
        public void AfterTest()
        {
            SFConfiguration.Instance().ChunkDownloaderVersion = ChunkDownloaderVersionDefault; // Return to default version
        }

        private QueryExecResponseData mockQueryRequestData()
        {
            return new QueryExecResponseData
            {
                rowSet = new string[,] { { } },
                rowType = new List<ExecResponseRowType>(),
                parameters = new List<NameValueParameter>(),
                chunks = new List<ExecResponseChunk>{new ExecResponseChunk()
                {
                    url = "fake",
                    uncompressedSize = 100,
                    rowCount = 1
                }}
            };
        }

        private SFResultSet mockSFResultSet(QueryExecResponseData responseData, CancellationToken token)
        {
            string connectionString = "user=user;password=password;account=account;";
            SFSession session = new SFSession(connectionString, new SessionPropertiesContext());
            List<NameValueParameter> list = new List<NameValueParameter>
            {
                new NameValueParameter { name = "CLIENT_PREFETCH_THREADS", value = "3" }
            };
            session.UpdateSessionParameterMap(list);

            return new SFResultSet(responseData, new SFStatement(session), token);
        }

        [Test, NonParallelizable]
        public void TestGetDownloader([Values(1, 2, 3, 4)] int chunkDownloaderVersion)
        {
            // Set configuration settings
            SFConfiguration.Instance().ChunkDownloaderVersion = chunkDownloaderVersion;

            CancellationToken token = new CancellationToken();

            if (chunkDownloaderVersion == 4)
            {
                Exception ex = Assert.Throws<Exception>(() => ChunkDownloaderFactory.GetDownloader(null, null, token));
                Assert.AreEqual("Unsupported Chunk Downloader version specified in the SFConfiguration", ex.Message);
            }
            else
            {
                QueryExecResponseData responseData = mockQueryRequestData();
                SFResultSet resultSet = mockSFResultSet(responseData, token);

                IChunkDownloader downloader = ChunkDownloaderFactory.GetDownloader(responseData, resultSet, token);

                Assert.IsTrue(downloader is SFBlockingChunkDownloaderV3);
            }
        }
    }
}
