using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebBrowserRunner : IWebBrowserRunner
    {
        public static readonly WebBrowserRunner Instance = new WebBrowserRunner();

        internal WebBrowserRunner()
        {
        }

        public virtual void Run(Uri uri)
        {
            var url = uri.AbsoluteUri;
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
