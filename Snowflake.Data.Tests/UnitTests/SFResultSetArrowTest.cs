/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFResultSetArrowTest
    {
        [Test]
        [Ignore("SFResultSetArrowTest")]
        public void SFResultSetArrowTestDone()
        {
            // Do nothing;
        }
        
        private RecordBatch PrepareRecordBatch(int rows)
        {
            var range = Enumerable.Range(1, rows);
            return new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(range)))
                .Append("Col_Float", false, col => col.Float(array => array.AppendRange(range.Select(x => Convert.ToSingle(x * 2)))))
                .Append("Col_String", false, col => col.String(array => array.AppendRange(range.Select(x => $"Item {x}"))))
                .Append("Col_Boolean", false, col => col.Boolean(array => array.AppendRange(range.Select(x => x % 2 == 0))))
                .Build();
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
        
        [Test]
        public void TestResultSet()
        {
            var rowCount = 10;
            var recordBatch = PrepareRecordBatch(rowCount);
            
            var responseData = new QueryExecResponseData
            {
                rowType = recordBatch.Schema.FieldsList
                    .Select(col => 
                        new ExecResponseRowType
                        {
                            name = col.Name, 
                            type = "TEXT"
                        }).ToList(),
                parameters = new List<NameValueParameter>(),
                chunks = null, // TODO - add tests with multiple chunks
                queryResultFormat = ResultFormat.Arrow,
                rowsetBase64 = ConvertToBase64String(recordBatch)
            };
            
            SFSession session = new SFSession("user=user;password=password;account=account;", null);
            var sfStatement = new SFStatement(session);

            var resultSet = new SFResultSetArrow(responseData, sfStatement, new CancellationToken());
            
            for (var i = 0; i < rowCount; ++i)
            {
                Assert.IsTrue(resultSet.Next());
                Assert.AreEqual((i+1).ToString(), resultSet.GetString(0));
            }
            Assert.IsFalse(resultSet.Next());
        }

    }
}
