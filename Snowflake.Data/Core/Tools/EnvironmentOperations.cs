/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Core.Tools
{
    internal class EnvironmentOperations
    {
        public static readonly EnvironmentOperations Instance = new EnvironmentOperations();

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
