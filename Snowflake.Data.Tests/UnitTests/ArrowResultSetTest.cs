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
        private const int RowCount = 10;
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
            _recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, RowCount))))
                .Build();
            var responseData = PrepareResponseData(_recordBatch);
            var sfStatement = PrepareStatement();

            _arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());
        }
        
        [Test]
        public void TestNextReturnsTrueUntilRowsExist()
        {
            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(_arrowResultSet.Next());
            }
            Assert.IsFalse(_arrowResultSet.Next());
        }

        [Test]
        public async Task TestNextAsyncReturnsTrueUntilRowsExist()
        {
            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(await _arrowResultSet.NextAsync());
            }
            Assert.IsFalse(await _arrowResultSet.NextAsync());
        }

        [Test]
        public void TestNextResultReturnsFalse()
        {
            Assert.IsFalse(_arrowResultSet.NextResult());
        }

        [Test]
        public async Task TestNextResultAsyncReturnsFalse()
        {
            Assert.IsFalse(await _arrowResultSet.NextResultAsync(CancellationToken.None));
        }

        [Test]
        public void TestHasRowsReturnsTrueIfRowExists()
        {
            Assert.IsTrue(_arrowResultSet.HasRows());
        }

        [Test]
        public void TestHasRowsReturnsFalseIfNoRows()
        {
            _recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.Clear()))
                .Build();
            var responseData = PrepareResponseData(_recordBatch);
            var sfStatement = PrepareStatement();

            _arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());
            Assert.IsFalse(_arrowResultSet.HasRows());
        }

        [Test]
        public void TestRewindReturnsFalseBeforeFirstRow()
        {
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsFalseForFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsTrueForSecondRowAndMovesToFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            _arrowResultSet.Next(); // move to second row
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsTrueForThirdRowAndMovesToFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            _arrowResultSet.Next(); // move to second row
            _arrowResultSet.Next(); // move to third row
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestGetObjectInternalReturnsProperValuesForFirstColumn()
        {
            const int ColumnIndex = 0;
            var columnValues = (Int32Array)_recordBatch.Column(ColumnIndex);
            for (var i = 0; i < RowCount; ++i)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(columnValues.GetValue(i).ToString(), _arrowResultSet.getObjectInternal(ColumnIndex).ToString());
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
                chunks = null,
                queryResultFormat = ResultFormat.ARROW,
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
