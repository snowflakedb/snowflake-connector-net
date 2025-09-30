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
        private QueryContextCache _qcc = null;
        private const long BaseReadTimestamp = 1668727958;
        private const string Context = "Some query context";
        private const long BaseId = 0;
        private const long BasePriority = 0;
        private const int MaxCapacity = 5;

        private long[] _expectedIDs;
        private long[] _expectedReadTimestamp;
        private long[] _expectedPriority;

        private void InitCache()
        {
            _qcc = new QueryContextCache(MaxCapacity);
        }

        private void InitCacheWithData()
        {
            InitCacheWithDataWithContext(Context);
        }

        private void InitCacheWithDataWithContext(String context)
        {
            _qcc = new QueryContextCache(MaxCapacity);
            _expectedIDs = new long[MaxCapacity];
            _expectedReadTimestamp = new long[MaxCapacity];
            _expectedPriority = new long[MaxCapacity];
            for (int i = 0; i < MaxCapacity; i++)
            {
                _expectedIDs[i] = BaseId + i;
                _expectedReadTimestamp[i] = BaseReadTimestamp + i;
                _expectedPriority[i] = BasePriority + i;
                _qcc.Merge(_expectedIDs[i], _expectedReadTimestamp[i], _expectedPriority[i], context);
            }
            _qcc.SyncPriorityMap();
        }

        private void InitCacheWithDataInRandomOrder()
        {
            _qcc = new QueryContextCache(MaxCapacity);
            _expectedIDs = new long[MaxCapacity];
            _expectedReadTimestamp = new long[MaxCapacity];
            _expectedPriority = new long[MaxCapacity];
            for (int i = 0; i < MaxCapacity; i++)
            {
                _expectedIDs[i] = BaseId + i;
                _expectedReadTimestamp[i] = BaseReadTimestamp + i;
                _expectedPriority[i] = BasePriority + i;
            }

            _qcc.Merge(_expectedIDs[3], _expectedReadTimestamp[3], _expectedPriority[3], Context);
            _qcc.Merge(_expectedIDs[2], _expectedReadTimestamp[2], _expectedPriority[2], Context);
            _qcc.Merge(_expectedIDs[4], _expectedReadTimestamp[4], _expectedPriority[4], Context);
            _qcc.Merge(_expectedIDs[0], _expectedReadTimestamp[0], _expectedPriority[0], Context);
            _qcc.Merge(_expectedIDs[1], _expectedReadTimestamp[1], _expectedPriority[1], Context);
            _qcc.SyncPriorityMap();
        }

        private void AssertCacheData()
        {
            AssertCacheDataWithContext(Context);
        }

        private void AssertCacheDataWithContext(string context)
        {
            int size = _qcc.GetSize();
            Assert.IsTrue(size == MaxCapacity);

            // Compare elements
            SortedSet<QueryContextElement> elements = _qcc.getElements();
            int i = 0;
            foreach (QueryContextElement elem in elements)
            {
                Assert.AreEqual(_expectedIDs[i], elem.Id);
                Assert.AreEqual(_expectedReadTimestamp[i], elem.ReadTimestamp);
                Assert.AreEqual(_expectedPriority[i], elem.Priority);
                Assert.AreEqual(context, elem.Context);
                i++;
            }
            Assert.AreEqual(i, MaxCapacity);
        }

        [Test]
        public void TestIsEmpty()
        {
            InitCache();
            Assert.IsTrue(_qcc.GetSize() == 0);
        }

        [Test]
        public void TestWithSomeData()
        {
            InitCacheWithData();
            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestWithSomeDataInRandomOrder()
        {
            InitCacheWithDataInRandomOrder();
            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestMoreThanCapacity()
        {
            InitCacheWithData();

            // Add one more element at the end
            int i = MaxCapacity;
            _qcc.Merge(BaseId + i, BaseReadTimestamp + i, BasePriority + i, Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();

            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestChangingCapacity()
        {
            InitCacheWithData();

            // Add one more element at the end
            int i = MaxCapacity;
            _qcc.SetCapacity(MaxCapacity + 1);
            _qcc.Merge(BaseId + i, BaseReadTimestamp + i, BasePriority + i, Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();
            Assert.IsTrue(_qcc.GetSize() == MaxCapacity + 1);

            // reduce the capacity back
            _qcc.SetCapacity(MaxCapacity);
            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestUpdateTimestamp()
        {
            InitCacheWithData();

            // Add one more element with new TS with existing id
            int updatedID = 1;
            _expectedReadTimestamp[updatedID] = BaseReadTimestamp + updatedID + 10;
            _qcc.Merge(
                BaseId + updatedID, _expectedReadTimestamp[updatedID], BasePriority + updatedID, Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();

            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestUpdatePriority()
        {
            InitCacheWithData();

            // Add one more element with new priority with existing id
            int updatedID = 3;
            long updatedPriority = BasePriority + updatedID + 7;

            _expectedPriority[updatedID] = updatedPriority;
            _qcc.Merge(
                BaseId + updatedID, BaseReadTimestamp + updatedID, _expectedPriority[updatedID], Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();

            for (int i = updatedID; i < MaxCapacity - 1; i++)
            {
                _expectedIDs[i] = _expectedIDs[i + 1];
                _expectedReadTimestamp[i] = _expectedReadTimestamp[i + 1];
                _expectedPriority[i] = _expectedPriority[i + 1];
            }

            _expectedIDs[MaxCapacity - 1] = BaseId + updatedID;
            _expectedReadTimestamp[MaxCapacity - 1] = BaseReadTimestamp + updatedID;
            _expectedPriority[MaxCapacity - 1] = updatedPriority;

            AssertCacheData();
        }

        [Test]
        public void TestAddSamePriority()
        {
            InitCacheWithData();

            // Add one more element with same priority
            int i = MaxCapacity;
            long updatedPriority = BasePriority + 1;
            _qcc.Merge(BaseId + i, BaseReadTimestamp + i, updatedPriority, Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();
            _expectedIDs[1] = BaseId + i;
            _expectedReadTimestamp[1] = BaseReadTimestamp + i;

            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestAddSameIDButStaleTimestamp()
        {
            InitCacheWithData();

            // Add one more element with same priority
            int i = 2;
            _qcc.Merge(BaseId + i, BaseReadTimestamp + i - 10, BasePriority + i, Context);
            _qcc.SyncPriorityMap();
            _qcc.CheckCacheCapacity();

            // Compare elements
            AssertCacheData();
        }

        [Test]
        public void TestEmptyCacheWithNullData()
        {
            InitCacheWithData();

            _qcc.Update(null);
            Assert.AreEqual(_qcc.GetSize(), 0);
        }

        [Test]
        public void TestEmptyCacheWithEmptyResponseData()
        {
            InitCacheWithData();

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>("", JsonUtils.JsonSettings);
            _qcc.Update(rsp);
            Assert.AreEqual(_qcc.GetSize(), 0);
        }

        [Test]
        public void TestSerializeRequestAndDeserializeResponseData()
        {
            // Init _qcc
            InitCacheWithData();
            AssertCacheData();

            var json = JsonConvert.SerializeObject(_qcc.GetQueryContextResponse(), JsonUtils.JsonSettings);

            // Clear _qcc
            _qcc.ClearCache();
            Assert.AreEqual(_qcc.GetSize(), 0);

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>(json, JsonUtils.JsonSettings);
            _qcc.Update(rsp);
            AssertCacheData();
        }

        [Test]
        public void TestSerializeRequestAndDeserializeResponseDataWithNullContext()
        {
            // Init _qcc
            InitCacheWithDataWithContext(null);
            AssertCacheDataWithContext(null);

            var json = JsonConvert.SerializeObject(_qcc.GetQueryContextResponse(), JsonUtils.JsonSettings);

            // Clear _qcc
            _qcc.ClearCache();
            Assert.AreEqual(_qcc.GetSize(), 0);

            ResponseQueryContext rsp = JsonConvert.DeserializeObject<ResponseQueryContext>(json, JsonUtils.JsonSettings);
            _qcc.Update(rsp);
            AssertCacheDataWithContext(null);
        }
    }
}
