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

        public static string Config(string logLevel, string logPath)
        {
            return @"{
                ""common"": {
                    ""log_level"": {logLevel},
                    ""log_path"": {logPath}
                }
            }"
                .Replace("{logLevel}", SerializeParameter(logLevel))
                .Replace("{logPath}", SerializeParameter(logPath))
                .Replace("\\", "\\\\");
        }

        private static string SerializeParameter(string parameter)
        {
            return parameter == null ? "null" : $"\"{parameter}\"";
        }

    }
}
