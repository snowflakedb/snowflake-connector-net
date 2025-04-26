namespace Snowflake.Data.Client
{
    public interface ISnowflakeCredentialManager
    {
        string GetCredentials(string key);

        void RemoveCredentials(string key);

        void SaveCredentials(string key, string token);
    }
}
