using System;
using System.Collections.Generic;
using System.Diagnostics;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Log;

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


    internal static void AddTelemetryEvent(this Activity activity, string name)
    {
        if (activity is null)
            return;

        if (string.IsNullOrEmpty(name))
            return;

        activity.AddEvent(new ActivityEvent(name));
    }

    public static void SetException(this Activity activity, Exception exception)
    {
        if (activity is null)
            return;

        var description = exception != null
            ? SecretDetector.MaskSecrets(exception.Message).maskedText
            : "Unknown error";

#if NET6_0_OR_GREATER
        activity.SetStatus(ActivityStatusCode.Error, description);
#endif
        activity.SetTag(TelemetryTags.StatusCode, "ERROR");
        activity.SetTag(TelemetryTags.StatusDescription, description);

        if (exception != null)
        {
            var tagsCollection = new ActivityTagsCollection
            {
                { TelemetryTags.Exception, exception.GetType().FullName },
                { TelemetryTags.ExceptionMessage, description },
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
