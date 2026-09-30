using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

[CollectionDefinition(nameof(LoginRedirectWiremockTestFixture), DisableParallelization = true)]
public sealed class LoginRedirectWiremockTestFixture : ICollectionFixture<LoginRedirectWiremockTestFixture.Fixture>
{
    public sealed class Fixture : IDisposable
    {
        internal WiremockRunner Runner { get; }

        public Fixture()
        {
            Runner = WiremockRunner.NewWiremock();
        }

        public void Dispose()
        {
            Runner.Stop();
        }
    }
}

[Collection(nameof(LoginRedirectWiremockTestFixture))]
public sealed class LoginRedirectWiremockTest
{
    private static readonly string s_mappingPath = Path.Combine("wiremock", "HttpUtil");
    private readonly LoginRedirectWiremockTestFixture.Fixture _fixture;

    public LoginRedirectWiremockTest(LoginRedirectWiremockTestFixture.Fixture fixture)
    {
        _fixture = fixture;
        _fixture.Runner.ResetMapping();
    }

    [SFTheory(SkipCondition.SkipOnJenkins)]
    [InlineData(300)]
    [InlineData(301)]
    [InlineData(302)]
    [InlineData(303)]
    [InlineData(307)]
    [InlineData(308)]
    public async Task TestLoginRedirectSameOriginSucceeds(int statusCode)
    {
        // arrange
        _fixture.Runner.AddMappings(Path.Combine(s_mappingPath, "login_redirect_same_origin.json"), new StringTransformations().ThenTransform("%ACCESS_TOKEN%", $"{statusCode}"));
        var httpClient = CreateHttpClientWithStrictRedirectPolicy();
        var request = CreateLoginRequest();

        // act
        var response = await httpClient.SendAsync(request, CancellationToken.None).ConfigureAwait(false);

        // assert
        Assert.True(response.IsSuccessStatusCode);
        var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var result = JsonConvert.DeserializeObject<LoginResponse>(json, JsonUtils.JsonSettings);
        Assert.True(result.success);
        Assert.Equal("sessionToken123", result.data.token);
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestLoginRedirectCrossOriginReturnsRawResponse()
    {
        // arrange
        _fixture.Runner.AddMappings(Path.Combine(s_mappingPath, "login_redirect_cross_origin.json"));
        var httpClient = CreateHttpClientWithStrictRedirectPolicy();
        var request = CreateLoginRequest();

        // act & assert: unsafe cross-origin redirect throws SecurityException
        var ex = await Assert.ThrowsAsync<SecurityException>(
            () => httpClient.SendAsync(request, CancellationToken.None)).ConfigureAwait(false);
        Assert.Contains("Unsafe redirect rejected", ex.Message);
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestLoginRedirectLoopThrowsSecurityException()
    {
        // arrange - same-origin redirect loop is bounded by the consecutive redirect hop counter
        _fixture.Runner.AddMappings(Path.Combine(s_mappingPath, "login_redirect_loop.json"));
        var httpClient = CreateHttpClientWithStrictRedirectPolicy();
        var request = CreateLoginRequest();

        // act & assert
        var ex = await Assert.ThrowsAsync<SecurityException>(
            () => httpClient.SendAsync(request, CancellationToken.None)).ConfigureAwait(false);
        Assert.Contains("Redirect loop detected", ex.Message);
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestLoginRedirectMultiHopSucceeds()
    {
        // arrange - 5 hops: origin -> absolute -> absolute -> relative -> relative -> absolute
        _fixture.Runner.AddMappings(Path.Combine(s_mappingPath, "login_redirect_multi_hop.json"), new StringTransformations().ThenTransform("%BASE_URL%", _fixture.Runner.WiremockBaseHttpsUrl));
        var httpClient = CreateHttpClientWithStrictRedirectPolicy();
        var request = CreateLoginRequest();

        // act
        var response = await httpClient.SendAsync(request, CancellationToken.None).ConfigureAwait(false);

        // assert
        Assert.True(response.IsSuccessStatusCode);
        var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var result = JsonConvert.DeserializeObject<LoginResponse>(json, JsonUtils.JsonSettings);
        Assert.True(result.success);
        Assert.Equal("multiHopToken", result.data.token);
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public async Task TestLoginRedirectMultiHopQueryOnlySucceeds()
    {
        // arrange - 3 hops where only query parameters change (?attempt=1, ?attempt=2, ?attempt=3).
        // Same-path-different-query redirects are followed, matching .NET's native auto-redirect.
        _fixture.Runner.AddMappings(Path.Combine(s_mappingPath, "login_redirect_multi_hop_query_only.json"), new StringTransformations().ThenTransform("%BASE_URL%", _fixture.Runner.WiremockBaseHttpsUrl));
        var httpClient = CreateHttpClientWithStrictRedirectPolicy();
        var request = CreateLoginRequest();

        // act
        var response = await httpClient.SendAsync(request, CancellationToken.None).ConfigureAwait(false);

        // assert
        Assert.True(response.IsSuccessStatusCode);
        var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var result = JsonConvert.DeserializeObject<LoginResponse>(json, JsonUtils.JsonSettings);
        Assert.True(result.success);
        Assert.Equal("queryOnlyHopToken", result.data.token);
    }

    [SFFact(SkipCondition.SkipOnJenkins)]
    public void TestAutoRedirectPolicyIsRejected()
    {
        // AllowAutoRedirect = true bypasses the driver's redirect safety checks
        var ex = Assert.Throws<SecurityException>(CreateHttpClientWithAutoRedirectPolicy);
        Assert.Contains("AllowAutoRedirect", ex.Message);
    }

    private static HttpClient CreateHttpClientWithStrictRedirectPolicy()
    {
        var config = new HttpClientConfig(null, null, null, null, null, false, false, 7, 20);
        var handler = new CustomDelegatingHandler(new HttpClientHandler
        {
            ClientCertificateOptions = ClientCertificateOption.Manual,
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
            AllowAutoRedirect = false,
        });
        return HttpUtil.Instance.CreateNewHttpClient(config, handler);
    }

    private static HttpClient CreateHttpClientWithAutoRedirectPolicy()
    {
        var config = new HttpClientConfig(null, null, null, null, null, false, false, 7, 20);
        var handler = new CustomDelegatingHandler(new HttpClientHandler
        {
            ClientCertificateOptions = ClientCertificateOption.Manual,
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
            AllowAutoRedirect = true,
        });
        return HttpUtil.Instance.CreateNewHttpClient(config, handler);
    }

    private HttpRequestMessage CreateLoginRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, $"{_fixture.Runner.WiremockBaseHttpsUrl}/session/v1/login-request?requestId=test-123");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "some-token");
        request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(30));
        request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(60));
        return request;
    }
}
