using System.IO;
using System.Security;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{

    /// <summary>
    /// Validates file paths used by StorageClient.
    /// </summary>
    internal static class PathValidator
    {
        /// <summary>
        /// Validates that the resolved destination path does not escape the base directory.
        /// Throws <see cref="SecurityException"/> if path resolves outside base directory.
        /// </summary>
        /// <param name="baseDirectory">The intended target directory for the file.</param>
        /// <param name="destinationRelativePath">The relative file name or path to validate.</param>
        internal static void ValidateFileDestinationPath(string baseDirectory, string destinationRelativePath)
        {
            var baseDir = Path.GetFullPath(baseDirectory);
            if (!baseDir.EndsWith(Path.DirectorySeparatorChar.ToString()))
                baseDir += Path.DirectorySeparatorChar;

            var fullDestPath = Path.GetFullPath(Path.Combine(baseDirectory, destinationRelativePath));
            if (!fullDestPath.StartsWith(baseDir, System.StringComparison.Ordinal))
                throw new SecurityException($"File name '{destinationRelativePath}' resolves outside the target directory.");
        }
    }
}
