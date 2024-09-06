/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */


namespace Snowflake.Data.Configuration
{
    using System.Text.Json.Serialization;

    internal class ClientConfig
    {
        [JsonPropertyName("common")]
        [JsonRequired]
        public ClientConfigCommonProps CommonProps { get; set; }
    }
}
