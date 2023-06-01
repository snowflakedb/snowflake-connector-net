/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Runtime.InteropServices;

namespace Snowflake.Data.Core;

public class HttpMessageHandlerFactoryProvider
{
    internal HttpMessageHandlerFactory createHttpMessageHandlerFactory()
    {
        if (IsRunningOnWindowsDotnet())
        {
            return new HttpMessageHandlerForWindowsDotnetFactory();
        }
        return new HttpMessageHandlerForOtherFactory();
    }

    internal bool IsRunningOnWindowsDotnet()
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && SFEnvironment.ClientEnv.IsNetFramework;
    }
}