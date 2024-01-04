namespace Snowflake.Data.Core.Session
{
    internal interface ICreateSessionTokens
    {
        CreateSessionToken BeginCreate();

        void EndCreate(CreateSessionToken token);

        int Count();
    }
}