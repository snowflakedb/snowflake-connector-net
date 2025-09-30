using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal static class TempUtil
    {
        private static readonly string s_tempDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        static TempUtil()
        {
            DirectoryOperations.Instance.CreateDirectory(s_tempDir);
        }

        public static string GetTempPath()
        {
            if (!Directory.Exists(s_tempDir))
            {
                DirectoryOperations.Instance.CreateDirectory(s_tempDir);
            }

            return s_tempDir;
        }
    }
}
