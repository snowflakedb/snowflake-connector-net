using System;
using System.Diagnostics;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Telemetry;

/// <summary>
/// Provides helpers for starting <see cref="Activity"/> instances enriched
/// with Snowflake session context (warehouse, role, database, session id).
/// Activities are created with <see cref="ActivityKind.Client"/> and tagged
/// following OpenTelemetry semantic conventions.
/// </summary>
public static class ActivityStarter
{
    internal const string ActivitySourceName = "Snowflake_dotnet_activity";
    internal const string ClientDefinedTelemetrySourceName = "Client_custom_activity";

    private static readonly ActivitySource s_internalActivitySource = new(ActivitySourceName, SFEnvironment.DriverVersion);
    private static readonly ActivitySource s_customActivitySource = new(ClientDefinedTelemetrySourceName, SFEnvironment.DriverVersion);

    /// <summary>
    /// Starts a client-side <see cref="Activity"/> using the custom activity source,
    /// enriched with session context from the command's open connection.
    /// </summary>
    /// <param name="command">
    /// A <see cref="SnowflakeDbCommand"/> whose connection must be open and
    /// have client telemetry enabled.
    /// </param>
    /// <param name="name">The operation name for the activity.</param>
    /// <returns>A started <see cref="Activity"/>, or <c>null</c> if no listener is sampling.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the connection is not open or client telemetry is not enabled.
    /// </exception>
    public static Activity StartActivity(this SnowflakeDbCommand command, string name)
    {
        if (command?.Connection is not SnowflakeDbConnection connection || !connection.IsOpen())
            throw new ArgumentException($"{nameof(StartActivity)} can only be called on open connections!");

        if (!connection.SfSession.IsClientTelemetryEnabled())
            throw new ArgumentException($"Client telemetry needs to be enabled to call {nameof(StartActivity)}!");

        return StartActivity(connection.SfSession, name, s_customActivitySource);
    }

    internal static Activity StartActivity(this SFSession session, string name) => StartActivity(session, name, s_internalActivitySource);

    private static Activity StartActivity(this SFSession session, string name, ActivitySource activitySource)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException(@"Activity name cannot be null or empty.", nameof(name));

        var activity = activitySource.StartActivity(name, ActivityKind.Client);
        if (activity is null)
            return null;

        activity.SetSessionProperties(session);

        return activity;
    }
}
