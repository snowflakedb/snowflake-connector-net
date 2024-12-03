/*
 //* Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Log
{
    public interface SFAppender
    {
        void Append(string logLevel, string message, Type type, Exception ex = null);
    }
}
