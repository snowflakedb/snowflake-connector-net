/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core.CredentialManager
{
    internal enum TokenType
    {
        [StringAttr(value = "ID_TOKEN")]
        IdToken
    }

    public interface ISFCredentialManager
    {
        string GetCredentials(string key);

        void RemoveCredentials(string key);

        void SaveCredentials(string key, string token);
    }
}
