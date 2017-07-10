using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net.Http;

namespace Snowflake.Data.Core
{
    interface IRestRequest
    {
        JObject post(RestRequest postRequest);

        HttpResponseMessage get(S3DownloadRequest getRequest);
    }

    public class S3DownloadRequest
    {
        public Uri uri{ get; set; }

        public string qrmk { get; set; }
    }

    public class RestRequest
    {
        public Uri uri { get; set; }

        public Object jsonBody { get; set;  }

        public String authorizationToken { get; set; }
    }

    public class AuthnRequest
    {
        [JsonProperty(PropertyName = "data")]
        public AuthnRequestData data { get; set; }
    }

    public class AuthnRequestData
    {
        [JsonProperty(PropertyName = "CLIENT_APP_ID")]
        public String clientAppId { get; set; }

        [JsonProperty(PropertyName = "CLIENT_APP_VERSION")]
        public String clientAppVersion { get; set; }

        [JsonProperty(PropertyName = "ACCOUNT_NAME", NullValueHandling = NullValueHandling.Ignore)]
        public String accountName { get; set; }

        [JsonProperty(PropertyName = "LOGIN_NAME")]
        public String loginName { get; set; }

        [JsonProperty(PropertyName = "PASSWORD")]
        public String password { get; set; }
    }

    public class AuthnRequestClientEnv
    {
        [JsonProperty(PropertyName = "APPLICATION")]
        public String application { get; set; }

        [JsonProperty(PropertyName = "OS_VERSION")]
        public String osVersion { get; set; }
    }

    public class QueryRequest
    {
        [JsonProperty(PropertyName = "sqlText")]
        public string sqlText { get; set; }

        [JsonProperty(PropertyName = "describeOnly")]
        public bool describeOnly { get; set; }

        [JsonProperty(PropertyName = "bindings")]
        public ParameterBindings parameterBindings { get; set; }
    }
}
