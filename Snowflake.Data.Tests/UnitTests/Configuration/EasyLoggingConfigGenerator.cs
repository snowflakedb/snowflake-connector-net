using System.IO;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    public class EasyLoggingConfigGenerator
    {
        public const string EmptyConfig = "{}";
        public const string EmptyCommonConfig = @"{
            ""common"": {}
        }";
        
        public static readonly string WorkingDirectory = Path.Combine(".", "easy_logging_test_configs");
        
        public static string CreateConfigTempFile(string fileContent)
        {
            var filePath = NewConfigFilePath();
            using (var writer = File.CreateText(filePath))
            {
                writer.Write(fileContent);
            }

            return filePath;
        }

        private static string NewConfigFilePath()
        {
            return Path.Combine(WorkingDirectory, Path.GetRandomFileName());
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
                .Replace("{logPath}", SerializeParameter(logPath));
        }

        private static string SerializeParameter(string parameter)
        {
            return parameter == null ? "null" : $"\"{parameter}\"";
        }

    }
}
