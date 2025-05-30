namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigProvider
    {
        private readonly EasyLoggingConfigFinder _finder;

        private readonly EasyLoggingConfigParser _configParser;

        public static readonly EasyLoggingConfigProvider Instance = new EasyLoggingConfigProvider();

        internal EasyLoggingConfigProvider() : this(EasyLoggingConfigFinder.Instance, EasyLoggingConfigParser.Instance)
        {
        }

        internal EasyLoggingConfigProvider(EasyLoggingConfigFinder finder, EasyLoggingConfigParser configParser)
        {
            _finder = finder;
            _configParser = configParser;
        }

        public virtual ClientConfig ProvideConfig(string configFilePathFromConnectionString)
        {
            var filePath = _finder.FindConfigFilePath(configFilePathFromConnectionString);
            return filePath == null ? null : _configParser.Parse(filePath);
        }
    }
}
