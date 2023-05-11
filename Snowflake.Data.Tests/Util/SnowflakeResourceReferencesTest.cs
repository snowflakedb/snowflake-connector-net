using NUnit.Framework;
using Snowflake.Data.Util;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Util
{
    public class SnowflakeResourceReferencesTest
    {
        [Test]
        public void TestAdd()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource();
            refs.Add(res1);
            Assert.AreEqual(1, refs.Count);
            Assert.IsTrue(refs.TryGetValue<TestResource>(res1.ResourceID, out var actual1));
            Assert.True(object.ReferenceEquals(res1, actual1));

            var res2 = new TestResource();
            refs.Add(res2);
            Assert.AreEqual(2, refs.Count);
            Assert.IsTrue(refs.TryGetValue<TestResource>(res2.ResourceID, out var actual2));
            Assert.True(object.ReferenceEquals(res2, actual2));
        }

        [Test]
        public void TestRemove()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            var res2 = new TestResource(refs);

            refs.Remove(res1);
            Assert.AreEqual(1, refs.Count);
            Assert.IsFalse(refs.TryGetValue<TestResource>(res1.ResourceID, out var actual1));

            refs.Remove(res2);
            Assert.AreEqual(0, refs.Count);
            Assert.IsFalse(refs.TryGetValue<TestResource>(res2.ResourceID, out var actual2));
        }

        [Test]
        public void TestRemove_is_missing()
        {
            var refs = new SnowflakeResourceReferences();
            var notJoinedRes = new TestResource();

            Assert.DoesNotThrow(() => refs.Remove(notJoinedRes));
            Assert.AreEqual(0, refs.Count);
        }

        [Test]
        public void TryGetValue()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            var res2 = new TestResource(refs);

            Assert.IsTrue(refs.TryGetValue<TestResource>(res1.ResourceID, out var actual1));
            Assert.True(object.ReferenceEquals(res1, actual1));

            Assert.IsTrue(refs.TryGetValue<TestResource>(res2.ResourceID, out var actual2));
            Assert.True(object.ReferenceEquals(res2, actual2));
        }

        [Test]
        public void TryGetValue_is_missing()
        {
            var refs = new SnowflakeResourceReferences();

            Assert.IsFalse(refs.TryGetValue<TestResource>("missing", out var actual));
            Assert.IsNull(actual);
        }

        [Test]
        public void TryGetValue_is_already_gc()
        {
            var refs = new SnowflakeResourceReferences();
            string volatileResouceId = null;
            Task.Run(() =>
            {
                var res = new TestResource(refs);
                volatileResouceId = res.ResourceID;
            }).Wait();
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Assert.IsNotNull(volatileResouceId);
            Assert.AreEqual(1, refs.Count);
            Assert.IsFalse(refs.TryGetValue<TestResource>(volatileResouceId, out var actual));
            Assert.IsNull(actual);
        }

        [Test]
        public void TestDisposeAll()
        {
            var disposingOrder = new List<ISnowflakeResource>();

            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            res1.BeforeDispose += (sender, e) => disposingOrder.Add(sender);
            var res2 = new TestResource(refs);
            res2.BeforeDispose += (sender, e) => disposingOrder.Add(sender);

            refs.DisposeAll();

            Assert.IsTrue(refs.IsDisposed);
            Assert.AreEqual(0, refs.Count);
            Assert.AreEqual(1, res1.DispoedEvents.Count);
            Assert.AreEqual(1, res2.DispoedEvents.Count);
            Assert.AreEqual(2, disposingOrder.Count);
            Assert.AreEqual(res2.ResourceID, disposingOrder[0].ResourceID);
            Assert.AreEqual(res1.ResourceID, disposingOrder[1].ResourceID);
        }

        [Test]
        public void TestDisposeAll_empty()
        {
            var refs = new SnowflakeResourceReferences();

            refs.DisposeAll();

            Assert.IsTrue(refs.IsDisposed);
            Assert.AreEqual(0, refs.Count);
        }

        [Test]
        public void TestDisposeAll_thrownException()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            var res2 = new TestResource(refs);
            res2.BeforeDispose += (sender, e) => throw new Exception();
            var res3 = new TestResource(refs);

            Assert.Throws<Exception>(() => refs.DisposeAll());

            Assert.IsFalse(refs.IsDisposed);
            Assert.AreEqual(2, refs.Count);
            Assert.IsFalse(refs.TryGetValue<ISnowflakeResource>(res3.ResourceID, out var actual3));
            Assert.AreEqual(0, res1.DispoedEvents.Count);
            Assert.AreEqual(0, res2.DispoedEvents.Count);
            Assert.AreEqual(1, res3.DispoedEvents.Count);
        }

        [Test]
        public void TestDisposeAllSilently()
        {
            var refs = new SnowflakeResourceReferences();
            new TestResource(refs);
            new TestResource(refs);

            var actual = refs.DisposeAllSilently();

            Assert.IsTrue(actual);
            Assert.IsTrue(refs.IsDisposed);
            Assert.AreEqual(0, refs.Count);
        }

        [Test]
        public void TestDisposeAllSilently_thrownException()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            var res2 = new TestResource(refs);
            res2.BeforeDispose += (sender, e) => throw new Exception();
            var res3 = new TestResource(refs);

            var actual = refs.DisposeAllSilently();

            Assert.IsFalse(actual);
            Assert.IsFalse(refs.IsDisposed);
            Assert.AreEqual(2, refs.Count);
            Assert.IsFalse(refs.TryGetValue<ISnowflakeResource>(res3.ResourceID, out var actual3));
        }

        [Test]
        public void TestDispose()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            var res2 = new TestResource(refs);

            refs.Dispose();

            Assert.IsTrue(refs.IsDisposed);
            Assert.AreEqual(0, refs.Count);
            Assert.AreEqual(1, res1.DispoedEvents.Count);
            Assert.AreEqual(1, res2.DispoedEvents.Count);
        }

        [Test]
        public void TestDispose_thrownException()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            res1.BeforeDispose += (sender, e) => throw new Exception();
            var res2 = new TestResource(refs);

            Assert.DoesNotThrow(() => refs.Dispose());

            Assert.IsFalse(refs.IsDisposed);
            Assert.AreEqual(1, refs.Count);
            Assert.AreEqual(0, res1.DispoedEvents.Count);
            Assert.AreEqual(1, res2.DispoedEvents.Count);
        }

        [Test]
        public void TestDispose_doubleCall()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);

            refs.Dispose();
            Assert.AreEqual(0, refs.Count);
            Assert.AreEqual(1, res1.DispoedEvents.Count);

            refs.Dispose();
            Assert.AreEqual(0, refs.Count);
            Assert.AreEqual(1, res1.DispoedEvents.Count);
        }

        [Test]
        public void TestDispose_afterAdd()
        {
            var refs = new SnowflakeResourceReferences();
            var res1 = new TestResource(refs);
            refs.Dispose();
            Assert.IsTrue(refs.IsDisposed);

            var res2 = new TestResource(refs);
            Assert.IsFalse(refs.IsDisposed);
            refs.Dispose();

            Assert.IsTrue(refs.IsDisposed);
            Assert.AreEqual(0, refs.Count);
            Assert.AreEqual(1, res1.DispoedEvents.Count);
            Assert.AreEqual(1, res2.DispoedEvents.Count);
        }

        public class TestResource : ISnowflakeResource
        {
            public string ResourceID { get; } = Guid.NewGuid().ToString();

            public event SnowflakeResourceEventHandler Disposed;

            public event SnowflakeResourceEventHandler BeforeDispose;

            public List<(ISnowflakeResource, EventArgs)> DispoedEvents { get; } = new List<(ISnowflakeResource, EventArgs)>();

            public TestResource()
            {
                Disposed += (sender, e) => this.DispoedEvents.Add((sender, e));
            }

            public TestResource(SnowflakeResourceReferences refs) : this()
            {
                refs.Add(this);
            }

            public void Dispose()
            {
                if (BeforeDispose != null)
                {
                    BeforeDispose(this, EventArgs.Empty);
                }
                Disposed(this, EventArgs.Empty);
            }
        }
    }
}
