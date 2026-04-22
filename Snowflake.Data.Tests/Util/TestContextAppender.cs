using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.Util
{
    /// <summary>
    /// Appender that collects Snowflake driver log output in memory, keyed by test name.
    /// Logs can be flushed per test via <see cref="FlushLogs"/> on failure.
    /// </summary>
    internal sealed class TestContextAppender : SFAppender
    {
        internal PatternLayout PatternLayout { get; set; }

        private readonly ConcurrentDictionary<string, List<string>> _logsByTest =
            new ConcurrentDictionary<string, List<string>>();

        public void ActivateOptions() { }

        public void Append(string logLevel, string message, Type type, Exception ex = null)
        {
            var testName = TestContext.CurrentContext.Test.FullName;
            var logs = _logsByTest.GetOrAdd(testName, _ => new List<string>());

            var formattedMessage = PatternLayout.Format(logLevel, message, type);
            logs.Add(formattedMessage);

            if (ex == null)
                return;

            var maskedExceptionString = SecretDetector.MaskSecrets(ex.ToString()).maskedText;
            logs.Add(maskedExceptionString);
        }

        /// <summary>
        /// Drains all buffered log messages for the given test and returns them as a single string.
        /// The test's buffer is removed after this call.
        /// </summary>
        internal string FlushLogs(string testName)
        {
            if (!_logsByTest.TryRemove(testName, out var logs))
                return string.Empty;

            var builder = new StringBuilder();
            logs.ForEach(x => builder.AppendLine(x));
            return builder.ToString();
        }
    }
}
