using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

// A GCS upload session uses one credential style throughout: presigned URLs or a
// downscoped access token, never a mix. The two styles differ in how many stage
// resolutions the connector performs, which these tests pin by counting
// /queries/v1/query-request POSTs:
//   - Presigned: stageInfo has no access token, so the connector re-resolves the
//     PUT once per destination file to mint each object's presigned URL
//     (1 initial resolution + N per-file refreshes).
//   - Downscoped token: stageInfo.creds.GCS_ACCESS_TOKEN is folder-scoped and
//     covers every file, so the connector resolves exactly once regardless of
//     file count and never re-resolves.
[CollectionDefinition(nameof(PutGcsStageResolutionWiremockFixture), DisableParallelization = true)]
public sealed class PutGcsStageResolutionWiremockFixture : ICollectionFixture<PutGcsStageResolutionWiremockFixture>, IDisposable
{
    internal readonly IWiremockRunner Runner;
    private readonly RemoteCertificateValidationCallback _previousCallback;

    public PutGcsStageResolutionWiremockFixture()
    {
        if (SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnJenkins).ShouldSkip)
        {
            Runner = new Mock<IWiremockRunner>().Object;
            return;
        }

        // Trust WireMock's self-signed certificate for WebRequest-based GCS uploads (HTTPS)
        _previousCallback = ServicePointManager.ServerCertificateValidationCallback;
        ServicePointManager.ServerCertificateValidationCallback = (_, _, _, _) => true;

        Runner = WiremockRunner.NewWiremock();
    }

    public void Dispose()
    {
        ServicePointManager.ServerCertificateValidationCallback = _previousCallback;
        Runner.Stop();
    }
}

[Collection(nameof(PutGcsStageResolutionWiremockFixture))]
public sealed class PutGcsStageResolutionWiremockTest
{
    private static readonly string s_mappingDir = Path.Combine("wiremock", "PutGcsStageResolution");
    private static readonly string s_loginMapping = Path.Combine(s_mappingDir, "login_success.json");
    private static readonly string s_presignedMapping = Path.Combine(s_mappingDir, "query_put_gcs_presigned_ok.json");
    private static readonly string s_downscopedMapping = Path.Combine(s_mappingDir, "query_put_gcs_downscoped_ok.json");
    private static readonly HttpClient s_http = new();

    private readonly IWiremockRunner _runner;

    public PutGcsStageResolutionWiremockTest(PutGcsStageResolutionWiremockFixture fixture)
    {
        _runner = fixture.Runner;
        _runner.ResetMapping();
        // Clear the request journal so assertions only see requests from the current test
        s_http.DeleteAsync($"{_runner.WiremockBaseHttpUrl}/__admin/requests").Result.EnsureSuccessStatusCode();
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestPresignedGcsPutSendsTwoQueryRequests()
    {
        // arrange
        var tmpFileName = $"gcs_presigned_{Guid.NewGuid():N}.txt";
        var tmpFilePath = Path.Combine(Path.GetTempPath(), tmpFileName);
        File.WriteAllText(tmpFilePath, "presigned-test-data");

        try
        {
            _runner.AddMappings(s_loginMapping);
            _runner.AddMappings(s_presignedMapping, new StringTransformations()
                .ThenTransform("{{putFileName}}", tmpFileName)
                .ThenTransform("{{putFilePath}}", tmpFilePath.Replace("\\", "\\\\"))
                .ThenTransform("{{wiremockHttpUrl}}", _runner.WiremockBaseHttpUrl));

            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildConnectionString();
            await conn.OpenAsync().ConfigureAwait(false);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"PUT file://{tmpFilePath} @~";

            // act
            cmd.ExecuteNonQuery();

            // assert — 2 query-requests: initial resolution + per-file presigned URL refresh
            var queryRequests = GetWiremockRequestsTo("/queries/v1/query-request");
            Assert.Equal(2, queryRequests.Count);

            // assert — 1 PUT to the fake GCS upload endpoint
            var uploadRequests = GetWiremockRequestsTo("/fake-gcs-upload", method: "PUT");
            Assert.Single(uploadRequests);
        }
        finally
        {
            File.Delete(tmpFilePath);
        }
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestDownscopedGcsPutSendsOneQueryRequest()
    {
        // arrange
        var tmpDir = Path.Combine(Path.GetTempPath(), $"gcs_downscoped_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmpDir);
        var fileNames = new[] { "a.txt", "b.txt", "c.txt" };
        foreach (var name in fileNames)
        {
            File.WriteAllText(Path.Combine(tmpDir, name), $"data-{name}");
        }

        try
        {
            var putGlob = Path.Combine(tmpDir, "*");

            _runner.AddMappings(s_loginMapping);
            _runner.AddMappings(s_downscopedMapping, new StringTransformations()
                .ThenTransform("{{putGlob}}", putGlob.Replace("\\", "\\\\"))
                .ThenTransform("{{wiremockHttpsUrl}}", _runner.WiremockBaseHttpsUrl));

            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildConnectionString();
            await conn.OpenAsync().ConfigureAwait(false);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"PUT file://{putGlob} @~";

            // act
            cmd.ExecuteNonQuery();

            // assert — exactly 1 query-request: single resolution, no per-file refresh
            var queryRequests = GetWiremockRequestsTo("/queries/v1/query-request");
            Assert.Single(queryRequests);

            // assert — 3 PUT requests to GCS bucket (one per file)
            var uploadRequests = GetWiremockRequestsTo("/gcs-bucket/", method: "PUT");
            Assert.Equal(3, uploadRequests.Count);
        }
        finally
        {
            Directory.Delete(tmpDir, recursive: true);
        }
    }

    private List<JToken> GetWiremockRequestsTo(string urlPath, string method = "POST")
    {
        var requestBody = JObject.FromObject(new
        {
            urlPathPattern = $"{urlPath}.*",
            method
        }).ToString();

        var response = s_http.PostAsync(
            _runner.WiremockBaseHttpUrl + "/__admin/requests/find",
            new StringContent(requestBody, Encoding.UTF8, "application/json")).Result;

        var json = JObject.Parse(response.Content.ReadAsStringAsync().Result);
        return (json["requests"] as JArray)?.ToList() ?? new List<JToken>();
    }

    private static string BuildConnectionString()
    {
        return new StringBuilder()
            .Append("account=testaccount;")
            .Append("user=test;")
            .Append("password=test;")
            .Append($"host={WiremockRunner.Host};")
            .Append($"port={WiremockRunner.DefaultHttpPort};")
            .Append("scheme=http;")
            .Append("poolingEnabled=false;")
            .ToString();
    }
}
