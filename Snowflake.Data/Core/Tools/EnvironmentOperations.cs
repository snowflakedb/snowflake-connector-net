/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class EnvironmentOperations
    {
        public static readonly EnvironmentOperations Instance = new EnvironmentOperations();
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EnvironmentOperations>();

        public virtual string GetEnvironmentVariable(string variable, string defaultValue = null)
        {
            return Environment.GetEnvironmentVariable(variable) ?? defaultValue;
        }

        public virtual string GetFolderPath(Environment.SpecialFolder folder)
        {
            return Environment.GetFolderPath(folder);
        }

        public virtual string GetExecutionDirectory()
        {
            var executablePath = Environment.GetCommandLineArgs()[0];
            var directoryName = string.IsNullOrEmpty(executablePath) ? null : Path.GetDirectoryName(executablePath);
            if (string.IsNullOrEmpty(directoryName))
            {
                s_logger.Warn("Unable to determine execution directory");
                return null;
            }
            return directoryName;
        }
    }
}
