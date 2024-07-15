/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core.Session
{
    /**
     * ChangedSessionBehavior describes what should happen to a session with a changed state (schema/role/database/warehouse) when it returns to the pool.
     */
    public enum ChangedSessionBehavior
    {
        OriginalPool,
        Destroy
    }
}
