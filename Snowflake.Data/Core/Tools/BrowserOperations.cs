using Snowflake.Data.Client;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

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
                startInfo.FileName = System.IO.Path.Combine(Environment.SystemDirectory, "rundll32.exe");
                startInfo.Arguments = $"url.dll,FileProtocolHandler {url}";
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
    }
}
