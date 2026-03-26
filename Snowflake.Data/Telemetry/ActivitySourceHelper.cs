using System;
using System.Diagnostics;
using System.Reflection;
using Snowflake.Data.Client;

namespace Snowflake.Data.Telemetry
{
    /// <summary>
    /// Central ActivitySource for Snowflake connector tracing.
    /// This class manages all tracing activities for the connector.
    /// </summary>
    internal static class ActivitySourceHelper
    {
        // Common span names
        public const string OperationConnect = "snowflake.connection.open";

        public static class Tags
        {
            public const string DbSystem = "db.system";
            public const string DbName = "db.name";
            public const string DbStatement = "db.statement";
            public const string DbWarehouse = "db.warehouse";
            public const string DbRole = "db.role";
            public const string StatusCode = "";

            public const string SessionId = "snowflake.session.id";
        }

        internal const string ActivitySourceName = "Snowflake.Data";
        internal const int StatementMaxLen = 300;

        private static readonly ActivitySource activitySource = CreateActivitySource();

        internal static Activity? StartActivity(this SnowflakeDbConnection connection, string name)
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            var activity = activitySource.StartActivity(name, ActivityKind.Client);

            if (activity is null)
                return null;

            activity.SetTag(Tags.DbWarehouse, connection.SfSession.warehouse);
            activity.SetTag(Tags.DbRole, connection.SfSession.role);
            activity.SetTag(Tags.DbName, connection.Database);
            return activity;
        }

        internal static void SetQuery(this Activity activity, string sql)
        {
            if (activity is null || sql is null)
                return;
            if (sql.Length > StatementMaxLen)
            {
                sql = sql.Substring(0, StatementMaxLen);
            }
            activity.SetTag(Tags.DbStatement, sql);
        }

        internal static void SetSuccess(this Activity activity)
        {
        #if NET6_0_OR_GREATER
            activity?.SetStatus(ActivityStatusCode.Ok);
        #endif
            activity?.SetTag(Tags.StatusCode, "OK");
            activity?.Stop();
        }

        internal static void SetException(this Activity activity, Exception exception)
        {
            if (exception is null) throw new ArgumentNullException(nameof(exception));

            var description = exception.Message;

            #if NET6_0_OR_GREATER
                activity?.SetStatus(ActivityStatusCode.Error, description);
            #endif

            activity?.SetTag("otel.status_code", "ERROR");
            activity?.SetTag("otel.status_description", description);
            activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception?.GetType().FullName },
                { "exception.message", exception?.Message },
            }));
            activity?.Stop();
        }

        private static ActivitySource CreateActivitySource()
        {
            var assembly = typeof(ActivitySourceHelper).Assembly;
            var version = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>().Version;
            return new ActivitySource(ActivitySourceName, version);
        }
    }
}
