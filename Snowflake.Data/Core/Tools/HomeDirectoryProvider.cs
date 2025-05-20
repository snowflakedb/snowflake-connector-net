using Snowflake.Data.Log;
using System;

namespace Snowflake.Data.Core.Tools
{
    internal class HomeDirectoryProvider
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<HomeDirectoryProvider>();

        public static string HomeDirectory(EnvironmentOperations _environmentOperations)
        {
            try
            {
                var homeDirectory = _environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile);
                if (string.IsNullOrEmpty(homeDirectory) || homeDirectory.Equals("/"))
                {
                    return null;
                }
                return homeDirectory;
            }
            catch (Exception e)
            {
                s_logger.Error($"Error while trying to retrieve the home directory: {e}");
                return null;
            }
        }
    }
}
