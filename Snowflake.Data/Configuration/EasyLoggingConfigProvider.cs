namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigProvider
    {
        private readonly EasyLoggingConfigFinder _finder;

        private readonly EasyLoggingConfigParser _configParser;

        public EasyLoggingConfigProvider(EasyLoggingConfigFinder finder, EasyLoggingConfigParser configParser)
        {
            _finder = finder;
            _configParser = configParser;
        }

        public EasyLoggingConfig ProvideConfig(string configFilePathFromConnectionString)
        {
            var filePath = _finder.FindConfigFilePath(configFilePathFromConnectionString);
            return filePath == null ? null : _configParser.Parse(filePath);
        }
    }
}
