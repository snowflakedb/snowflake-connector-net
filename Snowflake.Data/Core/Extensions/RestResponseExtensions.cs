namespace Snowflake.Data.Core.Extensions
{
    internal static class RestResponseExtensions
    {
        private const int SF_QUERY_IN_PROGRESS = 333333;
        private const int SF_QUERY_IN_PROGRESS_ASYNC = 333334;
        private const int SF_SESSION_EXPIRED_CODE = 390112;
        private const int SF_SESSION_GONE = 390111;

        public static bool IsQueryInProgress(this BaseRestResponse r) =>
            r.code == SF_QUERY_IN_PROGRESS || r.code == SF_QUERY_IN_PROGRESS_ASYNC;

        public static bool IsSessionExpired(this BaseRestResponse r) => r.code == SF_SESSION_EXPIRED_CODE;

        public static bool IsSessionGone(this BaseRestResponse r) => r.code == SF_SESSION_GONE;
    }
}
