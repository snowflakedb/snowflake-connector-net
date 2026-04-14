using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Telemetry
{
    /// <summary>
    /// Helper to generate and send telemetry events from exceptions.
    /// Follows the Go connector's exceptionTelemetry() pattern:
    /// a standalone function called at key throw sites, rather than
    /// embedding telemetry in the exception constructor.
    /// </summary>
    internal static class TelemetryHelper
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<TelemetryClient>();

        private const string SnowflakeNamespace = "Snowflake.Data";

        /// <summary>
        /// Main entry point: generate telemetry data from an exception and send it
        /// via the session's telemetry client. Silently no-ops if session is null,
        /// telemetry is disabled, or any error occurs.
        /// </summary>
        internal static void SendExceptionTelemetry(
            SnowflakeDbException ex,
            SFSession session,
            string eventType = TelemetryEventType.SqlException)
        {
            try
            {
                if (session?._telemetry == null || !session._telemetry.IsTelemetryEnabled())
                    return;

                var data = GenerateExceptionData(ex, eventType);
                session._telemetry.AddLog(data);
            }
            catch (Exception e)
            {
                // Telemetry must never affect exception propagation
                logger.Debug($"Failed to send exception telemetry: {e.Message}");
            }
        }

        /// <summary>
        /// Convenience method for throw sites: sends telemetry and returns the exception
        /// so callers can write: throw TelemetryHelper.SendAndThrow(ex, session);
        /// </summary>
        internal static SnowflakeDbException SendAndThrow(
            SnowflakeDbException ex,
            SFSession session,
            string eventType = TelemetryEventType.SqlException)
        {
            SendExceptionTelemetry(ex, session, eventType);
            return ex;
        }

        /// <summary>
        /// Generate a TelemetryData instance from an exception, matching the
        /// wire format used by JDBC/Go/Python connectors.
        /// </summary>
        internal static TelemetryData GenerateExceptionData(
            SnowflakeDbException ex,
            string eventType = TelemetryEventType.SqlException)
        {
            var message = new Dictionary<string, string>
            {
                { TelemetryField.Type, eventType },
                { TelemetryField.Source, SFEnvironment.DriverName },
                { TelemetryField.DriverType, SFEnvironment.DriverName },
                { TelemetryField.DriverVersion, SFEnvironment.DriverVersion }
            };

            if (!string.IsNullOrEmpty(ex.QueryId))
            {
                message[TelemetryField.QueryId] = ex.QueryId;
            }

            if (!string.IsNullOrEmpty(ex.SqlState))
            {
                message[TelemetryField.SqlState] = ex.SqlState;
            }

            if (ex.ErrorCode != 0)
            {
                message[TelemetryField.ErrorNumber] = ex.ErrorCode.ToString();
            }

            var maskedMessage = SecretDetector.MaskSecrets(ex.Message);
            if (!string.IsNullOrEmpty(maskedMessage.maskedText))
            {
                message[TelemetryField.Reason] = maskedMessage.maskedText;
            }

            // ex.StackTrace is null before the exception is thrown (SendAndThrow pattern).
            // Fall back to capturing the current call stack in that case.
            var rawStacktrace = ex.StackTrace ?? new StackTrace(true).ToString();
            var stacktrace = FilterStacktrace(rawStacktrace);
            if (!string.IsNullOrEmpty(stacktrace))
            {
                message[TelemetryField.Stacktrace] = stacktrace;
            }

            message[TelemetryField.Exception] = ex.GetType().Name;

            return new TelemetryData(message, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }

        /// <summary>
        /// Filter a stacktrace to only include frames from Snowflake.Data,
        /// truncate paths to hide user-specific filesystem prefixes,
        /// and mask any secrets that may appear in the trace.
        /// </summary>
        internal static string FilterStacktrace(string stacktrace)
        {
            if (string.IsNullOrEmpty(stacktrace))
                return string.Empty;

            var lines = stacktrace.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            var filtered = new StringBuilder();

            foreach (var line in lines)
            {
                if (line.Contains(SnowflakeNamespace))
                {
                    // Truncate file paths to start from Snowflake.Data to hide user-specific prefixes.
                    // A .NET stack frame looks like:
                    //   "   at Snowflake.Data.Core.SFStatement.Execute() in /Users/dev/repo/Snowflake.Data/Core/SFStatement.cs:line 200"
                    // We want to keep the method part and truncate the "in /path/" part.
                    var truncated = line;
                    var inIdx = line.IndexOf(" in ", StringComparison.Ordinal);
                    if (inIdx >= 0)
                    {
                        var pathPart = line.Substring(inIdx);
                        var nsIdx = pathPart.IndexOf(SnowflakeNamespace, StringComparison.Ordinal);
                        if (nsIdx > 0)
                        {
                            // Replace " in /full/path/Snowflake.Data/..." with " in Snowflake.Data/..."
                            truncated = line.Substring(0, inIdx) + " in " + pathPart.Substring(nsIdx);
                        }
                    }

                    // Strip any leading whitespace/path before the "at" keyword
                    var atIdx = truncated.IndexOf(" at ", StringComparison.Ordinal);
                    if (atIdx >= 0)
                    {
                        filtered.AppendLine(truncated.Substring(atIdx));
                    }
                    else
                    {
                        filtered.AppendLine(truncated);
                    }
                }
            }

            var result = filtered.ToString().TrimEnd();
            var masked = SecretDetector.MaskSecrets(result);
            return masked.maskedText ?? result;
        }
    }
}
