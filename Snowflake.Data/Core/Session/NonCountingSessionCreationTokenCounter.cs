namespace Snowflake.Data.Core.Session
{
    internal class NonCountingSessionCreationTokenCounter: ISessionCreationTokenCounter
    {
        private const int IrrelevantCreateSessionTimeout = 0; // in case of old caching pool or pooling disabled we do not remove expired ones nor even store them 
        
        public SessionCreationToken NewToken() => new SessionCreationToken(IrrelevantCreateSessionTimeout);

        public void RemoveToken(SessionCreationToken creationToken)
        {
        }

        public int Count() => 0;
    }
}
