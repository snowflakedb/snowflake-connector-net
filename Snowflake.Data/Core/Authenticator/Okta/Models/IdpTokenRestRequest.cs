using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta.Models
{
    internal class IdpTokenRestRequest : BaseRestRequest, IRestRequest
    {   
        private static readonly MediaTypeWithQualityHeaderValue JsonHeader = new MediaTypeWithQualityHeaderValue("application/json");

        internal IdpTokenRequest JsonBody { get; set; }
            
        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            var message = newMessage(method, Url);
            message.Headers.Accept.Add(JsonHeader);

            var json = JsonConvert.SerializeObject(JsonBody, JsonUtils.JsonSettings);
            message.Content = new StringContent(json, Encoding.UTF8, "application/json");

            return message;
        }
    }
}