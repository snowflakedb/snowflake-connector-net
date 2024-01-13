/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System.Diagnostics;

namespace Snowflake.Data.Log
{
    internal class EasyLoggerUtil
    {
        internal static int AllPermissions = 777;

        internal static int AllUserPermissions = 700;

        internal static int OnlyUserHasPermissionToWrite = 644;

        internal static string CallBash(string command)
        {
            using (Process process = new Process())
            {
                process.StartInfo.FileName = "/bin/bash";
                process.StartInfo.Arguments = $"-c \"{command}\"";
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.Start();
                return process.StandardOutput.ReadToEnd().Replace("\n", string.Empty);
            }
        }
    }
}
