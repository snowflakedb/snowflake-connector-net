using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using Xunit;

using TaskOrValueTask =
#if NET8_0_OR_GREATER
System.Threading.Tasks.ValueTask;
#else
System.Threading.Tasks.Task;
#endif


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
#pragma warning disable SYSLIB0014
        _previousCallback = ServicePointManager.ServerCertificateValidationCallback;
        ServicePointManager.ServerCertificateValidationCallback = (_, _, _, _) => true;
        var handler = new HttpClientHandler { ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator };
        var httpClient = new HttpClient(handler);
        Runner = WiremockRunner.NewWiremock(httpClient: httpClient);
    }

    public void Dispose()
    {
        ServicePointManager.ServerCertificateValidationCallback = _previousCallback;
#pragma warning restore SYSLIB0014
        Runner.Stop();
    }
}

[Collection(nameof(PutGcsStageResolutionWiremockFixture))]
public sealed class PutGcsStageResolutionWiremockTest : IAsyncLifetime
{
    private readonly PutGcsStageResolutionWiremockFixture _fixture;
    private static readonly string s_mappingDir = Path.Combine("wiremock", "PutGcsStageResolution");
    private static readonly string s_loginMapping = Path.Combine(s_mappingDir, "login_success.json");
    private static readonly string s_presignedMapping = Path.Combine(s_mappingDir, "query_put_gcs_presigned_ok.json");
    private static readonly string s_downscopedMapping = Path.Combine(s_mappingDir, "query_put_gcs_downscoped_ok.json");

    private IWiremockRunner _runner;

    public PutGcsStageResolutionWiremockTest(PutGcsStageResolutionWiremockFixture fixture)
    {
        _fixture = fixture;
    }

    [SFFact(SkipCondition.SkipOnJenkins, RetriesCount = RetriesCount.Thrice)]
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
            var queryRequests = await _fixture.Runner.GetWiremockRequestsToAsync("/queries/v1/query-request", "POST").ConfigureAwait(false);
            Assert.Equal(2, queryRequests.Count);

            // assert — 1 PUT to the fake GCS upload endpoint
            var uploadRequests = await _fixture.Runner.GetWiremockRequestsToAsync("/fake-gcs-upload", method: "PUT").ConfigureAwait(false);
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
        Skip.WhenOnTfm(Skip.Tfm.Net9 | Skip.Tfm.Net10 | Skip.Tfm.Net8, "Newer TFMs can't be controlled via ServicePointManager and this test might fail on SSL negotiation.");

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
            var queryRequests = await _fixture.Runner.GetWiremockRequestsToAsync("/queries/v1/query-request", "POST").ConfigureAwait(false);
            Assert.Single(queryRequests);

            // assert — 3 PUT requests to GCS bucket (one per file)
            var uploadRequests = await _fixture.Runner.GetWiremockRequestsToAsync("/gcs-bucket/", method: "PUT").ConfigureAwait(false);
            Assert.Equal(3, uploadRequests.Count);
        }
        finally
        {
            Directory.Delete(tmpDir, recursive: true);
        }
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

    public async TaskOrValueTask InitializeAsync()
    {
        _runner = _fixture.Runner;
        await _runner.ResetMappingAsync().ConfigureAwait(false);
    }

    public TaskOrValueTask DisposeAsync() => TaskOrValueTask.CompletedTask;
}
