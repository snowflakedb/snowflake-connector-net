/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal class IdpTokenRestRequest : BaseRestRequest, IRestRequest
    {
        private static readonly MediaTypeWithQualityHeaderValue s_jsonHeader = new MediaTypeWithQualityHeaderValue("application/json");

        internal IdpTokenRequest JsonBody { get; set; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = newMessage(method, Url);
            message.Headers.Accept.Add(s_jsonHeader);

            var json = JsonConvert.SerializeObject(JsonBody, JsonUtils.JsonSettings);
            message.Content = new StringContent(json, Encoding.UTF8, "application/json");

            return message;
        }
    }
}
