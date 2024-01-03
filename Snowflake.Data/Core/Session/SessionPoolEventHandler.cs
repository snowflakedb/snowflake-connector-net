namespace Snowflake.Data.Core.Session
{
    internal class SessionPoolEventHandler: ISessionPoolEventHandler
    {
        public virtual void OnNewSessionCreated(SessionPool sessionPool)
        {
        }

        public virtual void OnWaitingForSessionStarted(SessionPool sessionPool)
        {
        }

        public virtual void OnSessionProvided(SessionPool sessionPool)
        {
        }
    }
}
