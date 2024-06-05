/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    internal enum TokenType
    {
        [StringAttr(value = "ID_TOKEN")]
        IdToken
    }

    public interface ISnowflakeCredentialManager
    {
        string GetCredentials(string key);

        void RemoveCredentials(string key);

        void SaveCredentials(string key, string token);
    }
}
