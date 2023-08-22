/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class ArrowResultSetTest
    {
        private static int s_rowCount = 10;
        private RecordBatch _recordBatch;
        private ArrowResultSet _arrowResultSet;

        [Test]
        [Ignore("ArrowResultSetTest")]
        public void SFResultSetArrowTestDone()
        {
            // Do nothing - test progress marker
        }
        
        [SetUp]
        public void BeforeTest()
        {
            s_rowCount = 10;
            _recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, s_rowCount))))
                .Build();
            var responseData = PrepareResponseData(_recordBatch);
            var sfStatement = PrepareStatement();

            _arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());
        }
        
        [Test]
        public void TestNext()
        {
            var column = (Int32Array)_recordBatch.Column(0);
            for (var i = 0; i < s_rowCount; ++i)
            {
                Assert.IsTrue(_arrowResultSet.Next());
                Assert.AreEqual(column.GetValue(i), _arrowResultSet.GetValue<Int32>(0));
            }
            Assert.IsFalse(_arrowResultSet.Next());
        }

        [Test]
        public async Task TestNextAsync()
        {
            var column = (Int32Array)_recordBatch.Column(0);
            for (var i = 0; i < s_rowCount; ++i)
            {
                Assert.IsTrue(await _arrowResultSet.NextAsync());
                Assert.AreEqual(column.GetValue(i), _arrowResultSet.GetValue<Int32>(0));
            }
            Assert.IsFalse(await _arrowResultSet.NextAsync());
        }

        [Test]
        public void TestNextResult()
        {
            Assert.IsFalse(_arrowResultSet.NextResult());
        }

        [Test]
        public async Task TestNextResultAsync()
        {
            Assert.IsFalse(await _arrowResultSet.NextResultAsync(CancellationToken.None));
        }

        [Test]
        public void TestHasRows()
        {
            Assert.IsTrue(_arrowResultSet.HasRows());
        }

        [Test]
        public void TestRewind()
        {
            Assert.IsFalse(_arrowResultSet.Rewind());
            _arrowResultSet.Next();
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestGetObjectInternal()
        {
            var column = (Int32Array)_recordBatch.Column(0);
            for (var i = 0; i < s_rowCount; ++i)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(column.GetValue(i).ToString(), _arrowResultSet.getObjectInternal(0).ToString());
            }
        }
        
        private QueryExecResponseData PrepareResponseData(RecordBatch recordBatch)
        {
            return new QueryExecResponseData
            {
                rowType = recordBatch.Schema.FieldsList
                    .Select(col => 
                        new ExecResponseRowType
                        {
                            name = col.Name, 
                            type = "TEXT"
                        }).ToList(),
                parameters = new List<NameValueParameter>(),
                chunks = null, // TODO in SNOW-893835 - add tests with multiple chunks
                queryResultFormat = ResultFormat.Arrow,
                rowsetBase64 = ConvertToBase64String(recordBatch)
            };
        }

        private string ConvertToBase64String(RecordBatch recordBatch)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, recordBatch.Schema))
                {
                    writer.WriteRecordBatch(recordBatch);
                    writer.WriteEnd();
                }
                return Convert.ToBase64String(stream.ToArray());
            }
        }
        
        private SFStatement PrepareStatement()
        {
            SFSession session = new SFSession("user=user;password=password;account=account;", null);
            return new SFStatement(session);
        }
    }
}
