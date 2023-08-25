/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using System;
    using System.Collections.Generic;

    [TestFixture]
    class QueryContextCacheTest
    {
        private QueryContextCache qcc = null;
        private long BASE_READ_TIMESTAMP = 1668727958;
        private string CONTEXT = "Some query context";
        private long BASE_ID = 0;
        private long BASE_PRIORITY = 0;

        private int MAX_CAPACITY = 5;
        private long[] expectedIDs;
        private long[] expectedReadTimestamp;
        private long[] expectedPriority;

        private void initCache()
        {
            qcc = new QueryContextCache(MAX_CAPACITY);
        }

        private void initCacheWithData()
        {
            initCacheWithDataWithContext(CONTEXT);
        }

        private void initCacheWithDataWithContext(String context)
        {
            qcc = new QueryContextCache(MAX_CAPACITY);
            expectedIDs = new long[MAX_CAPACITY];
            expectedReadTimestamp = new long[MAX_CAPACITY];
            expectedPriority = new long[MAX_CAPACITY];
            for (int i = 0; i < MAX_CAPACITY; i++)
            {
                expectedIDs[i] = BASE_ID + i;
                expectedReadTimestamp[i] = BASE_READ_TIMESTAMP + i;
                expectedPriority[i] = BASE_PRIORITY + i;
                qcc.merge(expectedIDs[i], expectedReadTimestamp[i], expectedPriority[i], context);
            }
            qcc.syncPriorityMap();
        }

        private void initCacheWithDataInRandomOrder()
        {
            qcc = new QueryContextCache(MAX_CAPACITY);
            expectedIDs = new long[MAX_CAPACITY];
            expectedReadTimestamp = new long[MAX_CAPACITY];
            expectedPriority = new long[MAX_CAPACITY];
            for (int i = 0; i < MAX_CAPACITY; i++)
            {
                expectedIDs[i] = BASE_ID + i;
                expectedReadTimestamp[i] = BASE_READ_TIMESTAMP + i;
                expectedPriority[i] = BASE_PRIORITY + i;
            }

            qcc.merge(expectedIDs[3], expectedReadTimestamp[3], expectedPriority[3], CONTEXT);
            qcc.merge(expectedIDs[2], expectedReadTimestamp[2], expectedPriority[2], CONTEXT);
            qcc.merge(expectedIDs[4], expectedReadTimestamp[4], expectedPriority[4], CONTEXT);
            qcc.merge(expectedIDs[0], expectedReadTimestamp[0], expectedPriority[0], CONTEXT);
            qcc.merge(expectedIDs[1], expectedReadTimestamp[1], expectedPriority[1], CONTEXT);
            qcc.syncPriorityMap();
        }

        private void assertCacheData()
        {
            assertCacheDataWithContext(CONTEXT);
        }

        private void assertCacheDataWithContext(string context)
        {
            int size = qcc.getSize();
            Assert.IsTrue(size == MAX_CAPACITY);

            // Compare elements
            SortedSet<QueryContextElement> elements = qcc.getElements();
            int i = 0;
            foreach (QueryContextElement elem in elements)
            {
                Assert.AreEqual(expectedIDs[i], elem.id);
                Assert.AreEqual(expectedReadTimestamp[i], elem.readTimestamp);
                Assert.AreEqual(expectedPriority[i], elem.priority);
                Assert.AreEqual(context, elem.context);
                i++;
            }
            Assert.AreEqual(i, MAX_CAPACITY);
        }

        [Test]
        public void testIsEmpty()
        {
            initCache();
            Assert.IsTrue(qcc.getSize() == 0);
        }

        [Test]
        public void testWithSomeData()
        {
            initCacheWithData();
            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testWithSomeDataInRandomOrder()
        {
            initCacheWithDataInRandomOrder();
            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testMoreThanCapacity()
        {
            initCacheWithData();

            // Add one more element at the end
            int i = MAX_CAPACITY;
            qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i, BASE_PRIORITY + i, CONTEXT);
            qcc.syncPriorityMap();
            qcc.checkCacheCapacity();

            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testUpdateTimestamp()
        {
            initCacheWithData();

            // Add one more element with new TS with existing id
            int updatedID = 1;
            expectedReadTimestamp[updatedID] = BASE_READ_TIMESTAMP + updatedID + 10;
            qcc.merge(
                BASE_ID + updatedID, expectedReadTimestamp[updatedID], BASE_PRIORITY + updatedID, CONTEXT);
            qcc.syncPriorityMap();
            qcc.checkCacheCapacity();

            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testUpdatePriority()
        {
            initCacheWithData();

            // Add one more element with new priority with existing id
            int updatedID = 3;
            long updatedPriority = BASE_PRIORITY + updatedID + 7;

            expectedPriority[updatedID] = updatedPriority;
            qcc.merge(
                BASE_ID + updatedID, BASE_READ_TIMESTAMP + updatedID, expectedPriority[updatedID], CONTEXT);
            qcc.syncPriorityMap();
            qcc.checkCacheCapacity();

            for (int i = updatedID; i < MAX_CAPACITY - 1; i++)
            {
                expectedIDs[i] = expectedIDs[i + 1];
                expectedReadTimestamp[i] = expectedReadTimestamp[i + 1];
                expectedPriority[i] = expectedPriority[i + 1];
            }

            expectedIDs[MAX_CAPACITY - 1] = BASE_ID + updatedID;
            expectedReadTimestamp[MAX_CAPACITY - 1] = BASE_READ_TIMESTAMP + updatedID;
            expectedPriority[MAX_CAPACITY - 1] = updatedPriority;

            assertCacheData();
        }

        [Test]
        public void testAddSamePriority()
        {
            initCacheWithData();

            // Add one more element with same priority
            int i = MAX_CAPACITY;
            long UpdatedPriority = BASE_PRIORITY + 1;
            qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i, UpdatedPriority, CONTEXT);
            qcc.syncPriorityMap();
            qcc.checkCacheCapacity();
            expectedIDs[1] = BASE_ID + i;
            expectedReadTimestamp[1] = BASE_READ_TIMESTAMP + i;

            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testAddSameIDButStaleTimestamp()
        {
            initCacheWithData();

            // Add one more element with same priority
            int i = 2;
            qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i - 10, BASE_PRIORITY + i, CONTEXT);
            qcc.syncPriorityMap();
            qcc.checkCacheCapacity();

            // Compare elements
            assertCacheData();
        }

        [Test]
        public void testEmptyCacheWithNullData()
        {
            initCacheWithData();

            qcc.update(null);
            Assert.AreEqual(qcc.getSize(), 0);
        }

        [Test]
        public void testEmptyCacheWithEmptyResponseData()
        {
            initCacheWithData();

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>("", JsonUtils.JsonSettings);
            qcc.update(rsp);
            Assert.AreEqual(qcc.getSize(), 0);
        }

        [Test]
        public void testSerializeRequestAndDeserializeResponseData()
        {
            // Init qcc
            initCacheWithData();
            assertCacheData();

            var json = JsonConvert.SerializeObject(qcc.getQueryContextResponse(), JsonUtils.JsonSettings);

            // Clear qcc
            qcc.clearCache();
            Assert.AreEqual(qcc.getSize(), 0);

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>(json, JsonUtils.JsonSettings);
            qcc.update(rsp);
            assertCacheData();
        }

        [Test]
        public void testSerializeRequestAndDeserializeResponseDataWithNullContext()
        {
            // Init qcc
            initCacheWithDataWithContext(null);
            assertCacheDataWithContext(null);

            var json = JsonConvert.SerializeObject(qcc.getQueryContextResponse(), JsonUtils.JsonSettings);

            // Clear qcc
            qcc.clearCache();
            Assert.AreEqual(qcc.getSize(), 0);

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>(json, JsonUtils.JsonSettings);
            qcc.update(rsp);
            assertCacheDataWithContext(null);
        }
    }
}
