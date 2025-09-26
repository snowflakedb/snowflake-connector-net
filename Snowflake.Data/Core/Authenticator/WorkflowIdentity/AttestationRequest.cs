using System;
using System.Collections.Generic;
using System.Net.Http;
using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class AttestationRequest
    {
        [JsonIgnore]
        public HttpMethod HttpMethod { get; set; }

        [JsonProperty(PropertyName = "method")]
        public string Method
        {
            get => HttpMethod.ToString();
            set => HttpMethod = new HttpMethod(value);
        }

        [JsonIgnore]
        public Uri Uri { get; set; }

        [JsonProperty(PropertyName = "url")]
        public string Url
        {
            get => Uri.AbsoluteUri;
            set => Uri = new Uri(value);
        }

        [JsonProperty(PropertyName = "headers")]
        public Dictionary<string, string> Headers { get; set; }
    }
}
