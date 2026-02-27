/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal class IdpTokenRequest
    {
        [JsonProperty(PropertyName = "username")]
        internal String Username { get; set; }

        [JsonProperty(PropertyName = "password")]
        internal String Password { get; set; }
    }
}
