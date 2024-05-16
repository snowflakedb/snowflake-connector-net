/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class ClientConfig
    {
        [JsonProperty(Required = Required.Always, PropertyName = "common")]
        public ClientConfigCommonProps CommonProps { get; set; }
    }
}
