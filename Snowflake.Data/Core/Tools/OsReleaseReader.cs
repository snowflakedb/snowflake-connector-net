using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class OsReleaseReader
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OsReleaseReader>();

        internal const string OsReleasePath = "/etc/os-release";

        // Parses KEY=VALUE pairs where VALUE may be quoted or unquoted:
        // BUILD_ID=rolling -> ["BUILD_ID", "rolling"]
        // NAME="Arch Linux" -> ["NAME", "Arch Linux"]
        private static readonly Regex s_keyValueRegex = new Regex(
            @"^([A-Z0-9_]+)=(?:""([^""]*)""|(.*))\s*$",
            RegexOptions.Compiled);

        private static readonly HashSet<string> s_allowedKeys = new HashSet<string>(StringComparer.Ordinal)
        {
            "NAME",
            "PRETTY_NAME",
            "ID",
            "IMAGE_ID",
            "IMAGE_VERSION",
            "BUILD_ID",
            "VERSION",
            "VERSION_ID"
        };

        private static Dictionary<string, string> s_cachedOsDetails;
        private static readonly object s_lock = new object();

        /// <summary>
        /// Returns parsed OS details from /etc/os-release on Linux.
        /// Returns null on non-Linux platforms or when the file cannot be read.
        /// Results are cached after the first call.
        /// </summary>
        internal static Dictionary<string, string> GetOsDetails()
        {
            if (s_cachedOsDetails != null)
            {
                return s_cachedOsDetails;
            }

            lock (s_lock)
            {
                if (s_cachedOsDetails != null)
                {
                    return s_cachedOsDetails;
                }

                s_cachedOsDetails = ReadOsDetails();
                return s_cachedOsDetails;
            }
        }

        private static Dictionary<string, string> ReadOsDetails()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return null;
            }

            try
            {
                var contents = File.ReadAllText(OsReleasePath);
                return ParseOsReleaseContents(contents);
            }
            catch (Exception e)
            {
                s_logger.Debug($"Failed to read OS details from {OsReleasePath}: {e.Message}");
                return null;
            }
        }

        internal static Dictionary<string, string> ParseOsReleaseContents(string contents)
        {
            var result = new Dictionary<string, string>(StringComparer.Ordinal);
            if (string.IsNullOrEmpty(contents))
            {
                return result;
            }

            foreach (var line in contents.Split('\n'))
            {
                var match = s_keyValueRegex.Match(line);
                if (!match.Success)
                {
                    continue;
                }

                var key = match.Groups[1].Value;
                if (!s_allowedKeys.Contains(key))
                {
                    continue;
                }

                // Groups[2] is the quoted value, Groups[3] is the unquoted value.
                // One of them will be empty, the other will have the actual value.
                // Trim the unquoted value to handle potential \r from CRLF line endings.
                var value = match.Groups[2].Success ? match.Groups[2].Value : match.Groups[3].Value.Trim();
                result[key] = value;
            }

            return result;
        }

        /// <summary>
        /// Resets the cached OS details. Intended for use in tests only.
        /// </summary>
        internal static void ResetCache()
        {
            lock (s_lock)
            {
                s_cachedOsDetails = null;
            }
        }
    }
}
