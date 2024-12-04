/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Core.Tools
{
    internal class HomeDirectoryProvider
    {
        private static readonly ILogger s_logger = SFLoggerFactory.GetCustomLogger<HomeDirectoryProvider>();

        public static string HomeDirectory(EnvironmentOperations _environmentOperations) {
            try
            {
                var homeDirectory = _environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile);
                if (string.IsNullOrEmpty(homeDirectory) || homeDirectory.Equals("/"))
                {
                    return null;
                }
                return homeDirectory;
            }
            catch (Exception e)
            {
                s_logger.LogError($"Error while trying to retrieve the home directory: {e}");
                return null;
            }
        }
    }
}
