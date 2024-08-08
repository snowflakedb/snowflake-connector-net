/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Client;
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
#if NETFRAMEWORK
            // .net standard would pass here
            Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
#else
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
#endif
        }
    }
}
