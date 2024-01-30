using System;
using System.Net.Http;
using System.Net.Http.Headers;

namespace Snowflake.Data.Core.Authenticator.Okta.Models
{
    internal class SamlRestRequest : BaseRestRequest, IRestRequest
    {
        internal string OnetimeToken { set; get; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            var builder = new UriBuilder(Url)
            {
                Query = "RelayState=%2Fsome%2Fdeep%2Flink&onetimetoken=" + OnetimeToken
            };
            var message = newMessage(method, builder.Uri);

            message.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));

            return message;
        }
    }
}