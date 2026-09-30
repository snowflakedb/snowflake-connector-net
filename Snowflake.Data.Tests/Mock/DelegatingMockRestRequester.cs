using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    internal class DelegatingMockRestRequester : IMockRestRequester
    {
        private readonly IRestRequester _inner;

        public DelegatingMockRestRequester(HttpClient httpClient)
        {
            _inner = new RestRequester(httpClient);
        }

        public void setHttpClient(HttpClient httpClient)
        {
            // No-op: client configured at construction
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken) =>
            _inner.PostAsync<T>(postRequest, cancellationToken);

        public T Post<T>(IRestRequest postRequest) =>
            _inner.Post<T>(postRequest);

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) =>
            _inner.GetAsync<T>(request, cancellationToken);

        public T Get<T>(IRestRequest request) =>
            _inner.Get<T>(request);

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) =>
            _inner.GetAsync(request, cancellationToken);

        public HttpResponseMessage Get(IRestRequest request) =>
            _inner.Get(request);
    }
}
