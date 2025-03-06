using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class PutGetStageInfoTest
    {
        [Test]
        [TestCaseSource(nameof(TestCases))]
        public void TestGcsRegionalUrl(string region, bool useRegionalUrl, string endPoint, string expectedGcsEndpoint)
        {
            // arrange
            var stageInfo = CreateGcsStageInfo(region, useRegionalUrl, endPoint);

            // act
            var gcsCustomEndpoint = stageInfo.GcsCustomEndpoint();

            // assert
            Assert.AreEqual(expectedGcsEndpoint, gcsCustomEndpoint);
        }

        internal static IEnumerable<object[]> TestCases()
        {
            yield return new object[] { "US-CENTRAL1", false, null, null };
            yield return new object[] { "US-CENTRAL1", false, "", null };
            yield return new object[] { "US-CENTRAL1", false, "null", null };
            yield return new object[] { "US-CENTRAL1", false, "    ", null };
            yield return new object[] { "US-CENTRAL1", false, "example.com", "example.com" };
            yield return new object[] { "ME-CENTRAL2", false, null, "storage.me-central2.rep.googleapis.com" };
            yield return new object[] { "ME-CENTRAL2", true, null, "storage.me-central2.rep.googleapis.com" };
            yield return new object[] { "ME-CENTRAL2", true, "", "storage.me-central2.rep.googleapis.com" };
            yield return new object[] { "ME-CENTRAL2", true, "  ", "storage.me-central2.rep.googleapis.com" };
            yield return new object[] { "ME-CENTRAL2", true, "example.com", "example.com" };
            yield return new object[] { "US-CENTRAL1", true, null, "storage.us-central1.rep.googleapis.com" };
            yield return new object[] { "US-CENTRAL1", true, "", "storage.us-central1.rep.googleapis.com" };
            yield return new object[] { "US-CENTRAL1", true, " ", "storage.us-central1.rep.googleapis.com" };
            yield return new object[] { "US-CENTRAL1", true, "null", "storage.us-central1.rep.googleapis.com" };
            yield return new object[] { "US-CENTRAL1", true, "example.com", "example.com" };
        }

        private PutGetStageInfo CreateGcsStageInfo(string region, bool useRegionalUrl, string endPoint) =>
            new PutGetStageInfo
            {
                locationType = SFRemoteStorageUtil.GCS_FS,
                location = "some location",
                path = "some path",
                region = region,
                storageAccount = "some storage account",
                isClientSideEncrypted = true,
                stageCredentials = new Dictionary<string, string>(),
                presignedUrl = "some pre-signed url",
                endPoint = endPoint,
                useRegionalUrl = useRegionalUrl
            };
    }
}
