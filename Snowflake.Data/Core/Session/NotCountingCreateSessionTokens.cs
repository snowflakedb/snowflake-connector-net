namespace Snowflake.Data.Core.Session
{
    internal class NotCountingCreateSessionTokens: ICreateSessionTokens
    {
        private const int IrrelevantCreateSessionTimeout = 0;
        
        public CreateSessionToken BeginCreate() => new CreateSessionToken(IrrelevantCreateSessionTimeout);

        public void EndCreate(CreateSessionToken token)
        {
        }

        public int Count() => 0;
    }
}
