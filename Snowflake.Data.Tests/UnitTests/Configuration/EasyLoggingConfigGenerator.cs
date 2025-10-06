using System.IO;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    public class EasyLoggingConfigGenerator
    {
        public const string EmptyConfig = "{}";
        public const string EmptyCommonConfig = @"{
            ""common"": {}
        }";

        public static string CreateConfigTempFile(string workingDirectory, string fileContent)
        {
            var filePath = NewConfigFilePath(workingDirectory);
            using (var writer = File.CreateText(filePath))
            {
                writer.Write(fileContent);
            }

            return filePath;
        }

        private static string NewConfigFilePath(string workingDirectory)
        {
            return Path.Combine(workingDirectory, Path.GetRandomFileName());
        }

        public static string Config(string logLevel, string logPath, string logFileUnixPermissions = null)
        {
            var commonSection = $@"
                ""common"": {{
                    ""log_level"": {SerializeParameter(logLevel)},
                    ""log_path"": {SerializeParameter(logPath).Replace("\\", "\\\\")}
                }}";
            var dotnetSection = string.Empty;
            if (logFileUnixPermissions != null)
                dotnetSection = $@",
                ""dotnet"": {{
                    ""log_file_unix_permissions"": {SerializeParameter(logFileUnixPermissions)}
                }}";
            var config = "{" + commonSection + dotnetSection + "\n}";
            return config;
        }

        private static string SerializeParameter(string parameter)
        {
            return parameter == null ? "null" : $"\"{parameter}\"";
        }

    }
}
