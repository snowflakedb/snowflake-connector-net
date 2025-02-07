using System.Diagnostics;
using System.Runtime.InteropServices;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebBrowserRunner: IWebBrowserRunner
    {
        public static readonly WebBrowserRunner Instance = new WebBrowserRunner();

        internal WebBrowserRunner()
        {
        }

        public virtual void Run(string url)
        {
            // The following code is learnt from https://brockallen.com/2016/09/24/process-start-for-urls-on-net-core/
            // hack because of this: https://github.com/dotnet/corefx/issues/10361
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                url = url.Replace("&", "^&");
                Process.Start(new ProcessStartInfo("cmd", $"/c start {url}") { UseShellExecute = true });
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
