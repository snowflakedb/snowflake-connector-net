using System;
using System.Collections.Generic;
using System.Diagnostics;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Telemetry;

internal static class ActivityExtensions
{
    internal static void SetSessionProperties(this Activity activity, SFSession session)
    {
        activity.SetTag(TelemetryTags.DbSystem, TelemetryTags.Snowflake);
        activity.SetTag(TelemetryTags.DbWarehouse, session.warehouse);
        activity.SetTag(TelemetryTags.DbRole, session.role);
        activity.SetTag(TelemetryTags.DbName, session.database);
        activity.SetTag(TelemetryTags.SessionId, session.sessionId);
    }

    /// <summary>
    /// Marks the activity as successful, sets the status code tag to "OK", and stops the activity.
    /// </summary>
    /// <param name="activity">The activity to mark as successful. If null, the call is a no-op.</param>
    public static void SetSuccess(this Activity activity)
    {
        if (activity is null)
            return;

#if NET6_0_OR_GREATER
        activity.SetStatus(ActivityStatusCode.Ok);
#endif
        activity.SetTag(TelemetryTags.StatusCode, "OK");
        activity.Stop();
    }

    /// <summary>
    /// This API is intended for internal Snowflake client use and is subject to breaking changes without notice.
    /// Adds a named telemetry event to the activity with optional tags.
    /// </summary>
    /// <param name="activity">The activity to add the event to. If null, the call is a no-op.</param>
    /// <param name="name">The name of the event. If null or empty, the call is a no-op.</param>
    /// <param name="tags">Optional key-value pairs to attach to the event.</param>
    public static void AddTelemetryEvent(this Activity activity, string name, params IEnumerable<KeyValuePair<string, object>> tags)
    {
        if (activity is null)
            return;

        if (string.IsNullOrEmpty(name))
            return;

        activity.AddEvent(new ActivityEvent(name, tags: new ActivityTagsCollection(tags)));
    }

    /// <summary>
    /// Marks the activity as failed, records the exception details as an event, and stops the activity.
    /// </summary>
    /// <param name="activity">The activity to mark as failed. If null, the call is a no-op.</param>
    /// <param name="exception">The exception to record. If null, only the error status is set.</param>
    public static void SetException(this Activity activity, Exception exception)
    {
        if (activity is null)
            return;

#if NET6_0_OR_GREATER
        activity.SetStatus(ActivityStatusCode.Error, "ERROR");
#endif
        activity.SetTag(TelemetryTags.StatusCode, "ERROR");

        if (exception != null)
        {
            var tagsCollection = new ActivityTagsCollection
            {
                { TelemetryTags.Exception, exception.GetType().FullName },
            };

            if (TryGetErrorCode(exception, out var code))
                tagsCollection.Add(TelemetryTags.ExceptionErrorCode, code);

            activity.AddEvent(new ActivityEvent("exception", tags: tagsCollection));
        }

        activity.Stop();
    }

    private static bool TryGetErrorCode(Exception exception, out string snowflakeDbException)
    {
        if (exception is SnowflakeDbException dbEx)
        {
            snowflakeDbException = dbEx.ErrorCode.ToString();
            return true;
        }

        if (exception is not AggregateException aggEx)
        {
            snowflakeDbException = string.Empty;
            return false;
        }

        var aggregationExceptionsVisited = 0;
        var aggregateExceptions = new List<AggregateException> { aggEx };
        while (aggregateExceptions.Count > aggregationExceptionsVisited && aggregationExceptionsVisited < MaxAggregateExceptionDepth)
        {
            var currentAggException = aggregateExceptions[aggregationExceptionsVisited++];

            foreach (var innerException in currentAggException.InnerExceptions)
            {
                switch (innerException)
                {
                    case SnowflakeDbException nestedDbEx:
                        snowflakeDbException = nestedDbEx.ErrorCode.ToString();
                        return true;
                    case AggregateException aex:
                        aggregateExceptions.Add(aex);
                        break;
                }
            }
        }

        snowflakeDbException = string.Empty;
        return false;
    }

    private const int MaxAggregateExceptionDepth = 3;
}
