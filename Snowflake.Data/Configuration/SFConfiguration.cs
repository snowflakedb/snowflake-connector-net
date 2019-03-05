/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Configuration
{
    public class SFConfiguration
    {
        public Boolean UseV2JsonParser;

        public Boolean UseV2ChunkDownloader;

        private SFConfiguration()
        {
            UseV2JsonParser = true;
            UseV2ChunkDownloader = false;
        }

        private readonly static SFConfiguration Config = new SFConfiguration();

        public static SFConfiguration Instance()
        {
            return Config;
        }
    }
}
