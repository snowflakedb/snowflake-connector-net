/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Core.Tools
{
    internal class EnvironmentOperations
    {
        public static readonly EnvironmentOperations Instance = new EnvironmentOperations();
        private static readonly ILogger s_logger = SFLoggerFactory.GetLogger<EnvironmentOperations>();

        public virtual string GetEnvironmentVariable(string variable)
        {
            return Environment.GetEnvironmentVariable(variable);
        }

        public virtual string GetFolderPath(Environment.SpecialFolder folder)
        {
            return Environment.GetFolderPath(folder);
        }
    }
}
