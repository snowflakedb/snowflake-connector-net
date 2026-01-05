/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal class IdpTokenResponse
    {
        [JsonProperty(PropertyName = "cookieToken")]
        internal String CookieToken { get; set; }

        [JsonProperty(PropertyName = "sessionToken")]
        internal String SessionToken { get; set; }
    }
}
