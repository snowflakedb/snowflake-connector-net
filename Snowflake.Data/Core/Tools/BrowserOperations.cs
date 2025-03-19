using Snowflake.Data.Client;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32;

namespace Snowflake.Data.Core.Tools
{
    internal class BrowserOperations
    {
        public static readonly BrowserOperations Instance = new BrowserOperations();

        public virtual void OpenUrl(string url)
        {
            // The following code is learnt from https://brockallen.com/2016/09/24/process-start-for-urls-on-net-core/
            // hack because of this: https://github.com/dotnet/corefx/issues/10361
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.FileName = GetSystemDefaultBrowser(); // default browser extracted from registry
                startInfo.Arguments = url;
                startInfo.UseShellExecute = false;
                Process.Start(startInfo);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                Process.Start("open", url);
            }
            else
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_PLATFORM);
            }
        }

        // The following function is based on: https://stackoverflow.com/a/62006560
        internal string GetSystemDefaultBrowser()
        {
            RegistryKey regKey = Registry.ClassesRoot.OpenSubKey("HTTP\\shell\\open\\command", false);

            try
            {
                // get rid of the enclosing quotes
                string name = regKey.GetValue(null).ToString().ToLower().Replace("" + (char)34, "");

                if (!name.EndsWith("exe"))
                    //get rid of all command line arguments
                    name = name.Substring(0, name.LastIndexOf(".exe") + 4);
                return name;
            }
            finally
            {
                regKey.Close();
            }
        }
    }
}
