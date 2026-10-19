using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal abstract class MockBaseRestRequester : IMockRestRequester
{
    public virtual Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken) => throw new NotImplementedException();

    public virtual T Post<T>(IRestRequest postRequest) => throw new NotImplementedException();

    public virtual Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();

    public virtual T Get<T>(IRestRequest request) => throw new NotImplementedException();

    public virtual Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<HttpResponseMessage>(null);

    public virtual HttpResponseMessage Get(IRestRequest request) => null;

    public virtual void setHttpClient(HttpClient httpClient) { }

    public virtual HttpClient GetHttpClient() => null;
}
