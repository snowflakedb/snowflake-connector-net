/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Configuration
{
    public class SFConfiguration
    {
        public Boolean UseV2JsonParser;

        // Leave this configuration for backward compatibility.
        // We would discard it after we announce this change.
        // Right now, when this is true, it would disable the ChunkDownloaderVersion
        public Boolean UseV2ChunkDownloader;

        public int ChunkDownloaderVersion;

        private SFConfiguration()
        {
            UseV2JsonParser = true;
            UseV2ChunkDownloader = false;
            ChunkDownloaderVersion = 3;
        }

        private readonly static SFConfiguration Config = new SFConfiguration();

        public static SFConfiguration Instance()
        {
            return Config;
        }
    }
}
