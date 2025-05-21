using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.Util.TestData;

namespace Snowflake.Data.Tests.IcebergTests
{
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    [NonParallelizable]
    public class TestIcebergTable : SFBaseTest
    {
        private const string TableNameIceberg = "DOTNET_TEST_DATA_IB";
        private const string TableNameHybrid = "DOTNET_TEST_DATA_HY";
        private const string SqlCreateIcebergTableColumns = @"nu1 number(10,0),
              nu2 number(19,0),
              nu3 number(18,2),
              nu4 number(38,0),
              f float,
              tx varchar(16777216),
              bt boolean,
              bf boolean,
              dt date,
              tm time,
              ntz timestamp_ntz(6),
              ltz timestamp_ltz(6),
              bi binary(5),
              ar array(number(10,0)),
              ob object(a number(10,0), b varchar),
              ma map(varchar, varchar)";
        private const string SqlCreateHybridTableColumns = @"id number(10,0) not null primary key,
              nu number(10,0),
              tx2 varchar(100)";
        private const string IcebergTableCreateFlags = "external_volume = 'demo_exvol' catalog = 'snowflake' base_location = 'x/'";
        private const string SqlColumnsSimpleTypes = "nu1,nu2,nu3,nu4,f,tx,bt,bf,dt,tm,ntz,ltz,bi";
        private const string SqlColumnsHybridTypes = "id,nu,tx2";
        private const string SqlColumnsStructureTypes = "ar,ob,ma";
        private const int I32 = 1;
        private const long I64 = 9223372036854775807;
        private const decimal Dec = (decimal)2.67;
        private const double Dbl = 3.333e8;
        private const float Flt = -1.0e7f;
        private const string Txt = "Sample text";
        private const bool B1 = true;
        private const bool B0 = false;
        private const int Id1 = 1;
        private const int Id2 = 2;
        private const string Txt1 = "sample text for join1";
        private const string Txt2 = "sample text for join2";
        private static readonly DateTime s_ts = DateTime.ParseExact("2023/03/15 13:17:29.207", "yyyy/MM/dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
        private readonly DateTime _dt = s_ts.Date;
        private readonly DateTime _tm = s_ts;
        private readonly DateTime _ntz = s_ts;
        private readonly DateTimeOffset _ltz = DateTimeOffset.ParseExact("2023/03/15 13:17:29.207 +05:00", "yyyy/MM/dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture);
        private readonly byte[] _bi = Encoding.Default.GetBytes("flake");
        private readonly ResultFormat _resultFormat;
        private const string FormatYmd = "yyyy-MM-dd";
        private const string FormatHms = "HH:mm:ss";
        private const string FormatYmdHms = "yyyy-MM-dd HH:mm:ss";
        private const string FormatYmdHmsf = "yyyy-MM-dd HH:mm:ss.fffffff";
        private const string FormatYmdHmsfZ = "yyyy-MM-dd HH:mm:ss.fffffff zzz";

        public TestIcebergTable(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestInsertPlainText()
        {
            // Arrange
            using (var conn = OpenConnection())
            {
                CreateIcebergTable(conn);
                SetResultFormat(conn);

                // Act
                conn.ExecuteNonQuery(@$"insert into {TableNameIceberg} ({SqlColumnsSimpleTypes})
                                                    values ({I32}, {I64}, {Dec}, {Dbl}, {Flt}, '{Txt}', {B1}, {B0},
                                                            '{_dt.ToString(FormatYmd)}',
                                                            '{_tm.ToString(FormatHms)}',
                                                            '{_ntz.ToString(FormatYmdHms)}',
                                                            '{_ltz.ToString(FormatYmdHmsfZ)}',
                                                            '{ByteArrayToHexString(_bi)}')");

                // Assert
                var reader = conn.ExecuteReader($"select {SqlColumnsSimpleTypes} from {TableNameIceberg}");
                int rowsRead = 0;
                while (reader.Read())
                {
                    rowsRead++;
                    AssertRowValuesEqual(reader, SqlCreateIcebergTableColumns.Split('\n'), I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                }
                Assert.AreEqual(1, rowsRead);
            }
        }


        [Test]
        [Ignore("Not a scope for CICD tests")]
        public void TestInsertWithValueBinding()
        {
            // Arrange
            using (var conn = OpenConnection())
            {
                CreateIcebergTable(conn);
                SetResultFormat(conn);

                // Act
                InsertSingleRow(conn, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);

                // Assert
                var reader = conn.ExecuteReader($"select {SqlColumnsSimpleTypes} from {TableNameIceberg}");
                int rowsRead = 0;
                while (reader.Read())
                {
                    rowsRead++;
                    AssertRowValuesEqual(reader, SqlCreateIcebergTableColumns.Split('\n'), I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                }
                Assert.AreEqual(1, rowsRead);
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestUpdateWithValueBinding()
        {
            // Arrange
            var i32 = I32 * 2;
            var i64 = I32;
            var dec = Dec + (decimal)0.1;
            var dbl = Dbl / 16;
            var flt = Flt * 2.5;
            var txt = Txt + " updated";
            var b1 = !B1;
            var b0 = !B0;
            var dt = _dt.Add(TimeSpan.FromDays(3));
            var tm = _tm.AddMinutes(7);
            var ntz = _ntz.Add(TimeSpan.FromDays(10));
            var ltz = _ltz.Subtract(TimeSpan.FromSeconds(37));
            var bi = Encoding.Default.GetBytes("Snow");
            using (var conn = OpenConnection())
            {
                CreateIcebergTable(conn);
                SetResultFormat(conn);
                InsertSingleRow(conn, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);

                // Act
                using (var cmd = conn.CreateCommand($"update {TableNameIceberg} set nu1=?,nu2=?,nu3=?,nu4=?,f=?,tx=?,bt=?,bf=?,dt=?,tm=?,ntz=?,ltz=?,bi=? where nu1=? and (bt=? or dt=?)"))
                {
                    cmd.Add("1", DbType.Int32, i32);
                    cmd.Add("2", DbType.Int64, i64);
                    cmd.Add("3", DbType.Decimal, dec);
                    cmd.Add("4", DbType.Double, dbl);
                    cmd.Add("5", DbType.Double, flt);
                    cmd.Add("6", DbType.String, txt);
                    cmd.Add("7", DbType.Boolean, b1);
                    cmd.Add("8", DbType.Boolean, b0);
                    cmd.Add("9", DbType.Date, dt);
                    cmd.Add("10", DbType.Time, tm);
                    cmd.Add("11", DbType.DateTime, ntz);
                    cmd.Add("12", DbType.DateTime, ltz).SFDataType = SFDataType.TIMESTAMP_LTZ;
                    cmd.Add("13", DbType.Binary, bi);
                    cmd.Add("14", DbType.Int32, I32);
                    cmd.Add("15", DbType.Boolean, B1);
                    cmd.Add("16", DbType.Date, _dt);
                    Assert.AreEqual(1, cmd.ExecuteNonQuery());
                }

                // Assert
                var reader = conn.ExecuteReader($"select {SqlColumnsSimpleTypes} from {TableNameIceberg}");
                int rowsRead = 0;
                while (reader.Read())
                {
                    rowsRead++;
                    AssertRowValuesEqual(reader, SqlCreateIcebergTableColumns.Split('\n'), i32, i64, dec, dbl, flt, txt, b1, b0, dt, tm, ntz, ltz, bi);
                }
                Assert.AreEqual(1, rowsRead);
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestJoin()
        {
            using (var conn = OpenConnection())
            {
                // Arrange
                CreateIcebergTable(conn);
                CreateHybridTable(conn);
                InsertManyRows(conn, 10, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                InsertHybridTableData(conn);
                SetResultFormat(conn);

                // Act
                var sql = @$"select i.nu1,i.nu2,i.nu3,i.nu4,i.f,i.tx,i.bt,i.bf,i.dt,i.tm,i.ntz,i.ltz,i.bi, h.id,h.nu,h.tx2
                             from {TableNameIceberg} i
                             join {TableNameHybrid} h
                               on i.nu1 = h.nu order by i.nu1";

                // Assert
                var resultSetColumns = @"nu1 number(10,0),
                                    nu2 number(19,0),
                                    nu3 number(18,2),
                                    nu4 number(38,0),
                                    f float,
                                    tx varchar(16777216),
                                    bt boolean,
                                    bf boolean,
                                    dt date,
                                    tm time,
                                    ntz timestamp_ntz(6),
                                    ltz timestamp_ltz(6),
                                    bi binary(5),
                                    id number(10,0),
                                    nu number(10,0),
                                    tx2 varchar(100)".Split('\n');
                var reader = (DbDataReader)conn.ExecuteReader(sql);
                Assert.AreEqual(true, reader.Read());
                AssertRowValuesEqual(reader, resultSetColumns, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi, Id1, I32, Txt1);
                Assert.AreEqual(true, reader.Read());
                AssertRowValuesEqual(reader, resultSetColumns, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi, Id2, I32, Txt2);
                Assert.AreEqual(false, reader.Read());
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestDelete()
        {
            using (var conn = OpenConnection())
            {
                // Arrange
                CreateIcebergTable(conn);
                InsertManyRows(conn, 100, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                SetResultFormat(conn);

                // Act
                var cmd = conn.CreateCommand($"delete from {TableNameIceberg} where nu1 = ?");
                cmd.Add("1", DbType.Int32, I32);
                var removed = cmd.ExecuteReader();

                // Assert
                Assert.AreEqual(1, removed.RecordsAffected);
                var left = conn.ExecuteReader($"select count(*) from {TableNameIceberg} where nu1 <> {I32}");
                Assert.AreEqual(true, left.Read());
                Assert.AreEqual(99, left.GetInt32(0));
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestDeleteAll()
        {
            using (var conn = OpenConnection())
            {
                // Arrange
                CreateIcebergTable(conn);
                InsertManyRows(conn, 100, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                SetResultFormat(conn);

                // Act
                var cmd = conn.CreateCommand($@"delete from {TableNameIceberg}");
                var removed = cmd.ExecuteReader();

                // Assert
                Assert.AreEqual(100, removed.RecordsAffected);
                var left = conn.ExecuteReader($"select count(*) from {TableNameIceberg}");
                Assert.AreEqual(true, left.Read());
                Assert.AreEqual(0, left.GetInt32(0));
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestMultiStatement()
        {
            using (var conn = OpenConnection())
            {
                // Arrange
                CreateIcebergTable(conn);
                InsertSingleRow(conn, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                SetResultFormat(conn);

                // Act
                var cmd = conn.CreateCommand($"select * from {TableNameIceberg};select 1;select current_timestamp;select * from {TableNameIceberg}");
                cmd.Add("MULTI_STATEMENT_COUNT", DbType.Int32, 4);
                var reader = cmd.ExecuteReader();

                // Assert
                int rowsRead = 0;
                while (reader.Read())
                {
                    rowsRead++;
                    AssertRowValuesEqual((DbDataReader)reader, SqlCreateIcebergTableColumns.Split('\n'), I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);
                }
                Assert.AreEqual(1, rowsRead);
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestBatchInsertForLargeData()
        {
            using (var conn = OpenConnection())
            {
                // Arrange
                CreateIcebergTable(conn);
                SetResultFormat(conn);
                InsertManyRowsWithNulls(conn, 20_000, I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi);

                // Act
                var reader = conn.ExecuteReader($"select {SqlColumnsSimpleTypes} from {TableNameIceberg} order by nu1");

                // Assert
                var resultSetColumns = SqlCreateIcebergTableColumns.Split('\n');
                var expected = new object[] { I32, I64, Dec, Dbl, Flt, Txt, B1, B0, _dt, _tm, _ntz, _ltz, _bi };
                var rowsRead = 0;
                while (reader.Read())
                {
                    ++rowsRead;
                    expected[0] = rowsRead;
                    var expectedRow = NullEachNthValueBesidesFirst(expected, rowsRead - 1);
                    AssertRowValuesEqual(reader, resultSetColumns, expectedRow);
                }
                Assert.AreEqual(20_000, rowsRead);
            }
        }

        [Test]
        [Ignore("Not a scope for CICD")]
        public void TestStructuredTypesAsJsonString()
        {
            using (var conn = OpenConnection())
            {
                SetResultFormat(conn);
                CreateIcebergTable(conn);
                var sql = @$"insert into {TableNameIceberg} ({SqlColumnsStructureTypes})
                            select
                                [1,2,3]::ARRAY(number),
                                {{'a' : 1, 'b': 'two'}}::OBJECT(a number, b varchar),
                                {{'4':'one', '5': 'two', '6': 'three'}}::MAP(varchar, varchar)
                            ";
                conn.ExecuteNonQuery(sql);

                var dbDataReader = conn.ExecuteReader($"select {SqlColumnsStructureTypes} from {TableNameIceberg}");
                int rowsRead = 0;
                while (dbDataReader.Read())
                {
                    rowsRead++;
                    Assert.AreEqual("[1,2,3]", RemoveBlanks(dbDataReader.GetString(0)));
                    Assert.AreEqual("{\"a\":1,\"b\":\"two\"}", RemoveBlanks(dbDataReader.GetString(1)));
                    Assert.AreEqual("{\"4\":\"one\",\"5\":\"two\",\"6\":\"three\"}", RemoveBlanks(dbDataReader.GetString(2)));
                }
                Assert.AreEqual(1, rowsRead);
            }
        }

        private void CreateIcebergTable(SnowflakeDbConnection conn)
            => conn.ExecuteNonQuery($"create or replace iceberg table {TableNameIceberg} ({SqlCreateIcebergTableColumns}) {IcebergTableCreateFlags}");

        private void CreateHybridTable(SnowflakeDbConnection conn)
            => conn.ExecuteNonQuery($"create or replace hybrid table {TableNameHybrid} ({SqlCreateHybridTableColumns})");

        private void SetResultFormat(SnowflakeDbConnection conn)
            => conn.ExecuteNonQuery($"alter session set DOTNET_QUERY_RESULT_FORMAT={_resultFormat}");

        private SnowflakeDbConnection OpenConnection()
        {
            var conn = new SnowflakeDbConnection(ConnectionString);
            conn.Open();
            return conn;
        }

        private void InsertSingleRow(SnowflakeDbConnection conn, params object[] bindings)
        {
            Assert.AreEqual(13, bindings.Length);
            var sqlInsert = $"insert into {TableNameIceberg} ({SqlColumnsSimpleTypes}) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            using (var cmd = conn.CreateCommand(sqlInsert))
            {
                cmd.Add("1", DbType.Int32, bindings[0]);
                cmd.Add("2", DbType.Int64, bindings[1]);
                cmd.Add("3", DbType.Decimal, bindings[2]);
                cmd.Add("4", DbType.Double, bindings[3]);
                cmd.Add("5", DbType.Double, bindings[4]);
                cmd.Add("6", DbType.String, bindings[5]);
                cmd.Add("7", DbType.Boolean, bindings[6]);
                cmd.Add("8", DbType.Boolean, bindings[7]);
                cmd.Add("9", DbType.DateTime, bindings[8]);
                cmd.Add("10", DbType.DateTime, bindings[9]);
                cmd.Add("11", DbType.DateTime, bindings[10]);
                cmd.Add("12", DbType.DateTimeOffset, bindings[11]).SFDataType = SFDataType.TIMESTAMP_LTZ;
                cmd.Add("13", DbType.Binary, bindings[12]);
                Assert.AreEqual(1, cmd.ExecuteNonQuery());
            }
        }

        private void InsertManyRows(SnowflakeDbConnection conn, int times, params object[] bindings)
        {
            Assert.AreEqual(13, bindings.Length);
            var sqlInsert = $"insert into {TableNameIceberg} ({SqlColumnsSimpleTypes}) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            using (var cmd = conn.CreateCommand(sqlInsert))
            {
                cmd.Add("1", DbType.Int32, Enumerable.Range((int)bindings[0], times).ToArray());
                cmd.Add("2", DbType.Int64, Enumerable.Repeat((long)bindings[1], times).ToArray());
                cmd.Add("3", DbType.Decimal, Enumerable.Repeat((decimal)bindings[2], times).ToArray());
                cmd.Add("4", DbType.Double, Enumerable.Repeat((double)bindings[3], times).ToArray());
                cmd.Add("5", DbType.Double, Enumerable.Repeat((float)bindings[4], times).ToArray());
                cmd.Add("6", DbType.String, Enumerable.Repeat((string)bindings[5], times).ToArray());
                cmd.Add("7", DbType.Boolean, Enumerable.Repeat((bool)bindings[6], times).ToArray());
                cmd.Add("8", DbType.Boolean, Enumerable.Repeat((bool)bindings[7], times).ToArray());
                cmd.Add("9", DbType.DateTime, Enumerable.Repeat((DateTime)bindings[8], times).ToArray());
                cmd.Add("10", DbType.DateTime, Enumerable.Repeat((DateTime)bindings[9], times).ToArray());
                cmd.Add("11", DbType.DateTime, Enumerable.Repeat((DateTime)bindings[10], times).ToArray());
                cmd.Add("12", DbType.DateTimeOffset, Enumerable.Repeat((DateTimeOffset)bindings[11], times).ToArray())
                    .SFDataType = SFDataType.TIMESTAMP_LTZ;
                cmd.Add("13", DbType.Binary, Enumerable.Repeat((byte[])bindings[12], times).ToArray());
                Assert.AreEqual(times, cmd.ExecuteNonQuery());
            }
        }

        private void InsertManyRowsWithNulls(SnowflakeDbConnection conn, int times, params object[] bindings)
        {
            Assert.AreEqual(13, bindings.Length);
            var sqlInsert = $"insert into {TableNameIceberg} ({SqlColumnsSimpleTypes}) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            using (var cmd = conn.CreateCommand(sqlInsert))
            {
                cmd.Add("1", DbType.Int32, Enumerable.Range((int)bindings[0], times).ToArray());

                var longArray = Enumerable.Repeat((long?)bindings[1], times).ToArray();
                cmd.Add("2", DbType.Int64, NullEachNthValue(longArray, 2));

                var decArray = Enumerable.Repeat((decimal?)bindings[2], times).ToArray();
                cmd.Add("3", DbType.Decimal, NullEachNthValue(decArray, 3));

                var dblArray = Enumerable.Repeat((double?)bindings[3], times).ToArray();
                cmd.Add("4", DbType.Double, NullEachNthValue(dblArray, 4));

                var fltArray = Enumerable.Repeat((float?)bindings[4], times).ToArray();
                cmd.Add("5", DbType.Double, NullEachNthValue(fltArray, 5));

                var strArray = Enumerable.Repeat((string)bindings[5], times).ToArray();
                cmd.Add("6", DbType.String, NullEachNthValue(strArray, 6));

                var bltArray = Enumerable.Repeat((bool?)bindings[6], times).ToArray();
                cmd.Add("7", DbType.Boolean, NullEachNthValue(bltArray, 7));

                var blfArray = Enumerable.Repeat((bool?)bindings[7], times).ToArray();
                cmd.Add("8", DbType.Boolean, NullEachNthValue(blfArray, 8));

                var dtArray = Enumerable.Repeat((DateTime?)bindings[8], times).ToArray();
                cmd.Add("9", DbType.Date, NullEachNthValue(dtArray, 9));

                var tmArray = Enumerable.Repeat((DateTime?)bindings[9], times).ToArray();
                cmd.Add("10", DbType.Time, NullEachNthValue(tmArray, 10));

                var ntzArray = Enumerable.Repeat((DateTime?)bindings[10], times).ToArray();
                cmd.Add("11", DbType.DateTime, NullEachNthValue(ntzArray, 11));

                var ltzArray = Enumerable.Repeat((DateTimeOffset?)bindings[11], times).ToArray();
                cmd.Add("12", DbType.DateTimeOffset, NullEachNthValue(ltzArray, 12))
                    .SFDataType = SFDataType.TIMESTAMP_LTZ;

                var binArray = Enumerable.Repeat((byte[])bindings[12], times).ToArray();
                cmd.Add("13", DbType.Binary, NullEachNthValue(binArray, 13));

                Assert.AreEqual(times, cmd.ExecuteNonQuery());
            }
        }

        private void InsertHybridTableData(SnowflakeDbConnection conn)
        {
            using (var cmd = conn.CreateCommand($"insert into {TableNameHybrid} ({SqlColumnsHybridTypes}) values (?,?,?)"))
            {
                cmd.Add("1", DbType.Int32, new[] { Id1, Id2 });
                cmd.Add("2", DbType.Int32, new[] { I32, I32 });
                cmd.Add("3", DbType.String, new[] { Txt1, Txt2 });
                cmd.ExecuteNonQuery();
            }
        }

        private void AssertRowValuesEqual(DbDataReader actualRow, string[] columns, params object[] expectedRow)
        {
            foreach (var idx in Enumerable.Range(0, expectedRow.Length))
            {
                var expected = expectedRow[idx];
                if (expected is DBNull || expected == null)
                {
                    Assert.IsTrue(actualRow.IsDBNull(idx));
                    continue;
                }

                var column = columns[idx].ToUpper().Trim();
                var mismatch = $"Mismatch on column {idx}: {column}";
                switch (expected)
                {
                    case Int32 i32:
                        Assert.AreEqual(i32, actualRow.GetInt32(idx), mismatch);
                        break;
                    case Int64 i64:
                        Assert.AreEqual(i64, actualRow.GetInt64(idx), mismatch);
                        break;
                    case Decimal dec:
                        Assert.AreEqual(dec, actualRow.GetDecimal(idx), mismatch);
                        break;
                    case float flt:
                        Assert.AreEqual(flt, actualRow.GetFloat(idx), mismatch);
                        break;
                    case String str:
                        Assert.AreEqual(str, actualRow.GetString(idx), mismatch);
                        break;
                    case Boolean bl:
                        Assert.AreEqual(bl, actualRow.GetBoolean(idx), mismatch);
                        break;
                    case DateTime dt:
                        var frmt = column.Contains(" TIME") ? FormatHms : FormatYmdHmsf;
                        Assert.AreEqual(dt.ToString(frmt), actualRow.GetDateTime(idx).ToString(frmt), mismatch);
                        break;
                    case DateTimeOffset dto:
                        Assert.AreEqual(dto.ToUniversalTime().ToString(FormatYmdHmsfZ),
                                        actualRow.GetFieldValue<DateTimeOffset>(idx).ToUniversalTime().ToString(FormatYmdHmsfZ),
                                        mismatch);
                        break;
                    case byte[] bt:
                        Assert.AreEqual(bt, actualRow.GetFieldValue<byte[]>(idx), mismatch);
                        break;
                }
            }
        }
    }
}
