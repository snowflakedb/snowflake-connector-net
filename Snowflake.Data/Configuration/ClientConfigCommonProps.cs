/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Text.Json.Serialization;

namespace Snowflake.Data.Configuration
{
    internal class ClientConfigCommonProps
    {
        [JsonPropertyName("log_level")]
        public string LogLevel { get; set; }

        [JsonPropertyName("log_path")]
        public string LogPath { get; set; }
    }
}
