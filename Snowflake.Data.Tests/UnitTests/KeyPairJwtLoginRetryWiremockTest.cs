using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using Xunit;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;

namespace Snowflake.Data.Tests.UnitTests;

// SNOW-4138103: on a login 503 storm the retry handler must rebuild the body before
// each resend so key-pair auth presents a freshly-minted JWT, while non-key-pair auth
// resends an unchanged body.
[Collection(nameof(RestRequesterWiremockTestFixture))]
public sealed class KeyPairJwtLoginRetryWiremockTest
{
    private readonly IWiremockRunner _runner;
    private static readonly string s_mapping =
        Path.Combine("wiremock", "HttpUtil", "login_503_then_success.json");
    private const string LoginPath = "/session/v1/login-request";

    public KeyPairJwtLoginRetryWiremockTest(RestRequesterWiremockTestFixture fixture)
    {
        _runner = fixture.Runner;
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestFreshJwtMintedForEachLoginRetry()
    {
        await _runner.ResetMappingAsync().ConfigureAwait(false);
        _runner.AddMappings(s_mapping);

        var privateKeyFile = WritePkcs8RsaPrivateKeyToTempFile();
        try
        {
            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildKeyPairConnectionString(privateKeyFile);

            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);

            var loginRequests = await _runner
                .GetWiremockRequestsToAsync(LoginPath, "POST").ConfigureAwait(false);
            Assert.Equal(3, loginRequests.Count);

            var tokens = loginRequests.Select(ExtractJwt).ToList();
            Assert.All(tokens, token => Assert.False(string.IsNullOrEmpty(token)));
            Assert.Equal(3, tokens.Distinct().Count());
        }
        finally
        {
            File.Delete(privateKeyFile);
        }
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestNonKeypairLoginBodyUnchangedAcrossRetries()
    {
        await _runner.ResetMappingAsync().ConfigureAwait(false);
        _runner.AddMappings(s_mapping);

        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = BuildPasswordConnectionString();

        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(ConnectionState.Open, conn.State);

        var loginRequests = await _runner
            .GetWiremockRequestsToAsync(LoginPath, "POST").ConfigureAwait(false);
        Assert.Equal(3, loginRequests.Count);

        var bodies = loginRequests.Select(r => r["body"]?.ToString()).ToList();
        Assert.Single(bodies.Distinct());
    }

    private static string ExtractJwt(JToken recordedRequest)
    {
        var body = recordedRequest["body"]?.ToString();
        return JObject.Parse(body)["data"]?["TOKEN"]?.ToString();
    }

    private static string WritePkcs8RsaPrivateKeyToTempFile()
    {
        var generator = new RsaKeyPairGenerator();
        generator.Init(new KeyGenerationParameters(new SecureRandom(), 2048));
        var keyPair = generator.GenerateKeyPair();

        var path = Path.GetTempFileName();
        using (var writer = new StreamWriter(path))
        {
            var pemWriter = new PemWriter(writer);
            pemWriter.WriteObject(new Pkcs8Generator(keyPair.Private));
            pemWriter.Writer.Flush();
        }
        return path;
    }

    private string BuildKeyPairConnectionString(string privateKeyFile)
    {
        var uri = new Uri(_runner.WiremockBaseHttpUrl);
        return new StringBuilder()
            .Append("account=testaccount")
            .Append(";user=test")
            .Append(";authenticator=snowflake_jwt")
            .Append(";private_key_file=").Append(privateKeyFile)
            .Append(";host=").Append(uri.Host)
            .Append(";port=").Append(uri.Port)
            .Append(";scheme=").Append(uri.Scheme)
            .Append(";poolingEnabled=false")
            .Append(";")
            .ToString();
    }

    private string BuildPasswordConnectionString()
    {
        var uri = new Uri(_runner.WiremockBaseHttpUrl);
        return new StringBuilder()
            .Append("account=testaccount")
            .Append(";user=test")
            .Append(";password=test")
            .Append(";host=").Append(uri.Host)
            .Append(";port=").Append(uri.Port)
            .Append(";scheme=").Append(uri.Scheme)
            .Append(";poolingEnabled=false")
            .Append(";")
            .ToString();
    }
}
