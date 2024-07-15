namespace Snowflake.Data.Core.Session
{
    internal interface ISessionCreationTokenCounter
    {
        SessionCreationToken NewToken();

        void RemoveToken(SessionCreationToken creationToken);

        int Count();

        void Reset();
    }
}
