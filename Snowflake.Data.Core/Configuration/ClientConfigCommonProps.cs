/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class ClientConfigCommonProps
    {
        [JsonProperty(PropertyName = "log_level")]
        public string LogLevel { get; set; }
        
        [JsonProperty(PropertyName = "log_path")]
        public string LogPath { get; set; }
    }
}
