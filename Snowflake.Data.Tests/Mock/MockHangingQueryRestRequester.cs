using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Mock;

/// <summary>
/// Decorator around a real RestRequester that intercepts query execution and cancellation.
/// Login, session close, and all other calls are forwarded to the real requester.
///
/// Intercepted behavior:
///   - PostAsync with QueryRequest body → returns "in progress" (code 333333)
///   - GetAsync (polling loop) → hangs until cancellation token fires
///   - Sync Post with QueryCancelRequest body → hangs until <see cref="Dispose"/> is called
///
/// Dispose unblocks any hanging Post thread so the thread-pool thread is not leaked.
/// </summary>
internal sealed class MockHangingQueryRestRequester : IMockRestRequester, IDisposable
{
    private IRestRequester _inner;
    private readonly CancellationTokenSource _disposeCts = new();

    private const string MockQueryId = "mock-hanging-query-id-abc123";

    /// <summary>Fires when GetAsync starts hanging (waiting on cancellation).</summary>
    private ManualResetEventSlim GetAsyncHanging { get; } = new(false);

    /// <summary>Fires when sync Post (cancel) starts hanging.</summary>
    internal ManualResetEventSlim CancelPostHanging { get; } = new(false);

    /// <summary>True once the cancel Post call has been entered.</summary>
    internal bool CancelPostCalled { get; private set; }

    /// <summary>The cancel request captured by <see cref="Post{T}"/>, for asserting RestTimeout etc.</summary>
    internal SFRestRequest LastCancelRequest { get; private set; }

    public void setHttpClient(HttpClient httpClient)
    {
        _inner = new RestRequester(httpClient);
    }

    public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        var sfRequest = (SFRestRequest)request;

        if (sfRequest.jsonBody is not QueryRequest)
            return await _inner.PostAsync<T>(request, cancellationToken).ConfigureAwait(false);

        return (T)(object)new QueryExecResponse
        {
            success = true,
            code = 333333,
            data = new QueryExecResponseData
            {
                queryId = MockQueryId,
                getResultUrl = $"/queries/{MockQueryId}/result",
                rowType = [],
                parameters = []
            }
        };
    }

    public T Post<T>(IRestRequest request)
    {
        var sfRequest = (SFRestRequest)request;

        if (sfRequest.jsonBody is not QueryCancelRequest)
            return _inner.Post<T>(request);

        CancelPostCalled = true;
        LastCancelRequest = sfRequest;
        CancelPostHanging.Set();

        // Block until the mock is disposed.
        _disposeCts.Token.WaitHandle.WaitOne();
        return default;
    }

    public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        GetAsyncHanging.Set();
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        throw TrueException.ForNonTrueValue("This should be unreachable", null);
    }

    public T Get<T>(IRestRequest request) => _inner.Get<T>(request);

    public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) =>
        _inner.GetAsync(request, cancellationToken);

    public HttpResponseMessage Get(IRestRequest request) => _inner.Get(request);

    public void Dispose()
    {
        _disposeCts.Cancel();
        _disposeCts.Dispose();
        GetAsyncHanging.Dispose();
        CancelPostHanging.Dispose();
    }
}
