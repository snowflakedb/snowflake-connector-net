using System.IO;

namespace Snowflake.Data.Configuration
{
    internal class FileOperations
    {
        public virtual bool Exists(string path)
        {
            return File.Exists(path);
        }
    }
}
