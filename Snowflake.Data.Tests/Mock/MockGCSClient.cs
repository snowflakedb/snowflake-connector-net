/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.Mock
{
    class MockGCSClient
    {
        // Mock GCS data for FileHeader
        internal const string GcsIV = "MOCK_GCS";
        internal const string GcsKey = "MOCK_GCS_KEY";
        internal const string GcsMatdesc = "MOCK_GCS_MATDESC";
        internal const string SFCDigest = "MOCK_SFC_DIGEST";

        // Mock content length
        internal const int ContentLength = 9999;

        // Mock data for downloaded file
        internal const string GcsFileContent = "GCSClientTest";
    }
}
