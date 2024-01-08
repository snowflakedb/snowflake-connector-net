namespace Snowflake.Data.Core.Session
{
    internal class NonCountingSessionCreationTokenCounter: ISessionCreationTokenCounter
    {
        private const int IrrelevantCreateSessionTimeout = 0;
        
        public SessionCreationToken NewToken() => new SessionCreationToken(IrrelevantCreateSessionTimeout);

        public void RemoveToken(SessionCreationToken creationToken)
        {
        }

        public int Count() => 0;
    }
}
