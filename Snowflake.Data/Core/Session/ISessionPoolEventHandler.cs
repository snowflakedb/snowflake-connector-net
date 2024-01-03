namespace Snowflake.Data.Core.Session
{
    internal interface ISessionPoolEventHandler
    {
        void OnNewSessionCreated(SessionPool sessionPool);

        void OnWaitingForSessionStarted(SessionPool sessionPool);

        void OnSessionProvided(SessionPool sessionPool);
    }
}
