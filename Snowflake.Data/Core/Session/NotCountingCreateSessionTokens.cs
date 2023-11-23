namespace Snowflake.Data.Core.Session
{
    internal class NotCountingCreateSessionTokens: ICreateSessionTokens
    {
        public CreateSessionToken BeginCreate() => new CreateSessionToken(0);

        public void EndCreate(CreateSessionToken token)
        {
        }

        public int Count() => 0;
    }
}
