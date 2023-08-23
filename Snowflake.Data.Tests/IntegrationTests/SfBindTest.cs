/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using Newtonsoft.Json;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using Client;
    using Core;
    using System.Text;
    using System.Globalization;
    using System.Collections.Generic;

    [TestFixture]
    internal class SfBindTest : SFBaseTest
    {
        private readonly string _testName = TestContext.CurrentContext.Test.Name;
        
        [Test]
        [Ignore("SfBindTest")]
        public void SfBindTestDone()
        {
            // Do nothing;
        }
 
        [Test]
        public void TestArrayBind()
        {
            CreateOrReplaceTable(_testName, new []
            {
                "cola INTEGER",
                "colb STRING"
            });

            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {_testName} VALUES (?, ?)";

                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = new [] { 1, 2, 3 };
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = new [] { "str1", "str2", "str3" };
                    cmd.Parameters.Add(p2);

                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(3, count);
                }
            }
        }

        [Test]
        public void TestBindNullValue()
        {
            CreateOrReplaceTable(_testName, new[]
            {
                "intData NUMBER",
                "fixedNumericData NUMBER(10,1)",
                "floatingNumData DOUBLE",
                "stringData VARCHAR",
                "binaryData BINARY",
                "boolData BOOLEAN",
                "dateData DATE",
                "timeData TIME",
                "dateTimeData DATETIME",
                "dateTimeWithTimeZone TIMESTAMP_TZ"
            });
            
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                foreach (DbType type in Enum.GetValues(typeof(DbType)))
                {
                    var isTypeSupported = true;
                    string colName;
                    using (var cmd = conn.CreateCommand())
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = "p0";
                        param.DbType = type;

                        switch (type)
                        {
                            case DbType.SByte:
                            case DbType.Int16:
                            case DbType.Int32:
                            case DbType.Int64:
                            case DbType.Byte:
                            case DbType.UInt16:
                            case DbType.UInt32:
                            case DbType.UInt64:
                                colName = "intData";
                                break;
                            case DbType.Decimal:
                            case DbType.VarNumeric:
                                colName = "fixedNumericData";
                                break;
                            case DbType.Boolean:
                                colName = "boolData";
                                break;
                            case DbType.Double:
                            case DbType.Single:
                                colName = "floatingNumData";
                                break;
                            case DbType.Guid:
                            case DbType.String:
                            case DbType.StringFixedLength:
                                colName = "stringData";
                                break;
                            case DbType.Date:
                                colName = "dateData";
                                break;
                            case DbType.Time:
                                colName = "timeData";
                                break;
                            case DbType.DateTime:
                            case DbType.DateTime2:
                                colName = "dateTimeData";
                                break;
                            case DbType.DateTimeOffset:
                                colName = "dateTimeWithTimeZone";
                                break;
                            case DbType.Binary:
                                colName = "binaryData";
                                break;
                            default:
                                // Not supported
                                colName = "stringData";
                                isTypeSupported = false;
                                break;
                        }

                        if (isTypeSupported)
                        {
                            cmd.CommandText = $"INSERT INTO {_testName}({colName}) VALUES(:p0)";
                            param.Value = DBNull.Value;
                            cmd.Parameters.Add(param);
                            var rowsInserted = cmd.ExecuteNonQuery();
                            Assert.AreEqual(1, rowsInserted);
                        }
                        else
                        {
                            try
                            {
                                cmd.CommandText = $"INSERT INTO {_testName}(stringData) VALUES(:p0)";
                                param.Value = DBNull.Value;
                                cmd.Parameters.Add(param);
                                cmd.ExecuteNonQuery();
                            }
                            catch (SnowflakeDbException e)
                            {
                                Assert.AreEqual(270053, e.ErrorCode);
                            }
                        }
                    }

                    if (isTypeSupported)
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = $"SELECT {colName} FROM {_testName}";
                            using (var reader = cmd.ExecuteReader())
                            {
                                reader.Read();
                                Assert.IsTrue(reader.IsDBNull(0));
                                reader.Close();
                            }
                        }
                    }

                    // Clean up between each case
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"DELETE FROM {_testName}";
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        [Test]
        public void TestBindValue()
        {
            CreateOrReplaceTable(_testName, new[]
            {
                "intData NUMBER",
                "fixedNumericData NUMBER(10,1)",
                "floatingNumData DOUBLE",
                "stringData VARCHAR",
                "binaryData BINARY",
                "boolData BOOLEAN",
                "dateData DATE",
                "timeData TIME",
                "dateTimeData DATETIME",
                "dateTimeWithTimeZone TIMESTAMP_TZ"
            });
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                foreach (DbType type in Enum.GetValues(typeof(DbType)))
                {
                    var isTypeSupported = true;
                    string colName;
                    using (var cmd = conn.CreateCommand())
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = "p0";
                        param.DbType = type;

                        switch (type)
                        {
                            case DbType.SByte:
                            case DbType.Byte:
                                colName = "intData";
                                param.Value = 1;
                                break;
                            case DbType.Int16:
                            case DbType.Int32:
                            case DbType.Int64:
                            case DbType.UInt16:
                            case DbType.UInt32:
                            case DbType.UInt64:
                                colName = "intData";
                                param.Value = 10;
                                break;
                            case DbType.Decimal:
                            case DbType.VarNumeric:
                                colName = "fixedNumericData";
                                param.Value = 10.1;
                                break;
                            case DbType.Boolean:
                                colName = "boolData";
                                param.Value = true;
                                break;
                            case DbType.Double:
                            case DbType.Single:
                                colName = "floatingNumData";
                                param.Value = 2.5;
                                break;
                            case DbType.Guid:
                            case DbType.String:
                            case DbType.StringFixedLength:
                                colName = "stringData";
                                param.Value = "thisIsAString";
                                break;
                            case DbType.Date:
                                colName = "dateData";
                                param.Value = DateTime.Now;
                                break;
                            case DbType.Time:
                                colName = "timeData";
                                param.Value = DateTime.Now;
                                break;
                            case DbType.DateTime:
                            case DbType.DateTime2:
                                colName = "dateTimeData";
                                param.Value = DateTime.Now;
                                break;
                            case DbType.DateTimeOffset:
                                colName = "dateTimeWithTimeZone";
                                param.Value = DateTimeOffset.Now;
                                break;
                            case DbType.Binary:
                                colName = "binaryData";
                                param.Value = Encoding.UTF8.GetBytes("BinaryData");
                                break;
                            default:
                                // Not supported      
                                colName = "stringData";
                                isTypeSupported = false;
                                break;
                        }

                        if (isTypeSupported)
                        {
                            cmd.CommandText = $"INSERT INTO {_testName}({colName}) VALUES(:p0)";
                            cmd.Parameters.Add(param);
                            var rowsInserted = cmd.ExecuteNonQuery();
                            Assert.AreEqual(1, rowsInserted);
                        }
                        else
                        {
                            try
                            {
                                cmd.CommandText = $"INSERT INTO {_testName}(stringData) VALUES(:p0)";
                                param.Value = DBNull.Value;
                                cmd.Parameters.Add(param);
                                cmd.ExecuteNonQuery();
                            }
                            catch (SnowflakeDbException e)
                            {
                                Assert.AreEqual(270053, e.ErrorCode);
                            }
                        }
                    }

                    if (isTypeSupported)
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = $"SELECT {colName} FROM {_testName}";
                            using (var reader = cmd.ExecuteReader())
                            {
                                reader.Read();
                                Assert.IsTrue(!reader.IsDBNull(0));
                                reader.Close();
                            }
                        }
                    }

                    // Clean up between each case
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"DELETE FROM {_testName}";
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        [Test]
        public void TestBindValueWithSfDataType()
        {
            
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                foreach (SFDataType type in Enum.GetValues(typeof(SFDataType)))
                {
                    if (!type.Equals(SFDataType.None))
                    {
                        var isTypeSupported = true;
                        if (!type.Equals(SFDataType.FIXED))
                        {
                            CreateOrReplaceTable(_testName, new []
                            {
                                $"data {type.ToString()}",
                                "unsupportedType VARCHAR"
                            });
                        }
                        else
                        {
                            CreateOrReplaceTable(_testName, new []
                            {
                                "data NUMBER",
                                "unsupportedType VARCHAR"
                            });
                        }

                        using (var cmd = conn.CreateCommand())
                        {
                            var param = (SnowflakeDbParameter)cmd.CreateParameter();
                            param.ParameterName = "p0";
                            param.SFDataType = type;
                            switch (type)
                            {
                                case SFDataType.BINARY:
                                    param.Value = Encoding.UTF8.GetBytes("BinaryData");
                                    break;
                                case SFDataType.FIXED:
                                    param.Value = 10;
                                    break;
                                case SFDataType.BOOLEAN:
                                    param.Value = true;
                                    break;
                                case SFDataType.DATE:
                                    param.Value = DateTime.Now;
                                    break;
                                case SFDataType.TEXT:
                                    param.Value = "thisIsAString";
                                    break;
                                case SFDataType.TIMESTAMP_LTZ:
                                    param.Value = DateTimeOffset.Now;
                                    break;
                                case SFDataType.TIMESTAMP_NTZ:
                                    param.Value = DateTime.Now;
                                    break;
                                case SFDataType.TIMESTAMP_TZ:
                                    param.Value = DateTimeOffset.Now;
                                    break;
                                case SFDataType.TIME:
                                    param.Value = DateTime.Now;
                                    break;
                                case SFDataType.REAL:
                                    param.Value = 25.3;
                                    break;
                                default:
                                    isTypeSupported = false;
                                    param.Value = "InvalidSFDataType";
                                    break;
                            }

                            if (isTypeSupported)
                            {
                                // Set to an unsupported DB type to check that SFDataType has precedence
                                param.DbType = DbType.Object;
                                cmd.CommandText = $"INSERT INTO {_testName}(data) VALUES(:p0)";
                                cmd.Parameters.Add(param);
                                var rowsInserted = cmd.ExecuteNonQuery();
                                Assert.AreEqual(1, rowsInserted);
                            }
                            // DB rejects query if param type is VARIANT, OBJECT or ARRAY
                            else if (!type.Equals(SFDataType.VARIANT) && 
                                     !type.Equals(SFDataType.OBJECT) &&
                                     !type.Equals(SFDataType.ARRAY))
                            {
                                try
                                {
                                    cmd.CommandText = $"INSERT INTO {_testName}(unsupportedType) VALUES(:p0)";
                                    param.Value = DBNull.Value;
                                    cmd.Parameters.Add(param);
                                    cmd.ExecuteNonQuery();
                                }
                                catch (SnowflakeDbException e)
                                {
                                    Assert.AreEqual(270054, e.ErrorCode);
                                }
                            }
                        }

                        if (isTypeSupported)
                        {
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = $"SELECT data FROM {_testName}";
                                using (var reader = cmd.ExecuteReader())
                                {
                                    reader.Read();
                                    Assert.IsTrue(!reader.IsDBNull(0));
                                    reader.Close();
                                }
                            }
                        }
                    }
                }
            }
        }

        [Test]
        public void TestParameterCollection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p1.DbType = DbType.Int16;
                    p2.Value = 2;
                    

                    var p3 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p1.DbType = DbType.Int16;
                    p2.Value = 2;

                    var parameters = new IDbDataParameter[3];
                    parameters.SetValue(p1, 0);
                    parameters.SetValue(p2, 1);
                    parameters.SetValue(p3, 2);

                    ((SnowflakeDbParameterCollection)cmd.Parameters).AddRange(parameters);
                    Assert.Throws<NotImplementedException>(
                        () => { cmd.Parameters.CopyTo(parameters, 5); });
    
                    Assert.AreEqual(3, cmd.Parameters.Count);
                    Assert.IsTrue(cmd.Parameters.Contains(p2));
                    Assert.IsTrue(cmd.Parameters.Contains("2"));
                    Assert.AreEqual(1, cmd.Parameters.IndexOf(p2));
                    Assert.AreEqual(1, cmd.Parameters.IndexOf("2"));

                    cmd.Parameters.Remove(p2);
                    Assert.AreEqual(2, cmd.Parameters.Count);
                    Assert.AreSame(p1, cmd.Parameters[0]);

                    cmd.Parameters.RemoveAt(0); 
                    Assert.AreSame(p3, cmd.Parameters[0]);

                    cmd.Parameters.Clear();
                    Assert.AreEqual(0, cmd.Parameters.Count);
                }
            }
        }

        [Test]
        public void TestPutArrayBind()
        {
            CreateOrReplaceTable(_testName, new []
            {
                "cola INTEGER",
                "colb STRING",
                "colc DATE",
                "cold TIME",
                "cole TIMESTAMP_NTZ",
                "colf TIMESTAMP_TZ" 
            });
            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {_testName} values (?, ?, ?, ?, ?, ?)";

                    const int Total = 250000;

                    var arrInt = new List<int>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrInt.Add(i * 10 + 1);
                        arrInt.Add(i * 10 + 2);
                        arrInt.Add(i * 10 + 3);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrInt.ToArray();
                    cmd.Parameters.Add(p1);

                    var arrString = new List<string>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrString.Add("str1");
                        arrString.Add("str2");
                        arrString.Add("str3");
                    }
                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = arrString.ToArray();
                    cmd.Parameters.Add(p2);
                    
                    var date1 = DateTime.ParseExact("2000-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var date2 = DateTime.ParseExact("2020-05-11 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var date3 = DateTime.ParseExact("2021-07-22 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var arrDate = new List<DateTime>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrDate.Add(date1);
                        arrDate.Add(date2);
                        arrDate.Add(date3);
                    }
                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Date;
                    p3.Value = arrDate.ToArray();
                    cmd.Parameters.Add(p3);

                    var time1 = DateTime.ParseExact("00:00:00.0000000", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var time2 = DateTime.ParseExact("23:59:59.9999999", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var time3 = DateTime.ParseExact("12:35:41.3333333", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    var arrTime = new List<DateTime>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrTime.Add(time1);
                        arrTime.Add(time2);
                        arrTime.Add(time3);
                    }

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.Time;
                    p4.Value = arrTime.ToArray();
                    cmd.Parameters.Add(p4);

                    var ntz1 = DateTime.ParseExact("2017-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    var ntz2 = DateTime.ParseExact("2020-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    var ntz3 = DateTime.ParseExact("2022-04-01 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    var arrNtz = new List<DateTime>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrNtz.Add(ntz1);
                        arrNtz.Add(ntz2);
                        arrNtz.Add(ntz3);
                    }
                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.DateTime2;
                    p5.Value = arrNtz.ToArray();
                    cmd.Parameters.Add(p5);

                    var tz1 = DateTimeOffset.Now;
                    var tz2 = DateTimeOffset.UtcNow;
                    var tz3 = new DateTimeOffset(2007, 1, 1, 12, 0, 0, new TimeSpan(4, 0, 0));
                    var arrTz = new List<DateTimeOffset>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrTz.Add(tz1);
                        arrTz.Add(tz2);
                        arrTz.Add(tz3);
                    }

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.DateTimeOffset;
                    p6.Value = arrTz.ToArray();
                    cmd.Parameters.Add(p6);
                    
                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(Total * 3, count);

                    cmd.CommandText = $"SELECT * FROM {_testName}";
                    var reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }
            }
        }
        
        [Test]
        public void TestPutArrayBindWorkDespiteOfTypeNameHandlingAuto()
        {
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings {
                TypeNameHandling = TypeNameHandling.Auto
            };
            CreateOrReplaceTable(_testName, new []
            {
                "cola REAL",
                "colb TEXT",
                "colc NUMBER(38,0)"
            });
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {_testName} VALUES (?, ?, ?)";

                    const int Total = 250;
                    
                    var arrDouble = new List<double>();
                    var arrString = new List<string>();
                    var arrInt = new List<int>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrDouble.Add(i * 10 + 1);
                        arrDouble.Add(i * 10 + 2);
                        arrDouble.Add(i * 10 + 3);
                        
                        arrString.Add("stra"+i);
                        arrString.Add("strb"+i);
                        arrString.Add("strc"+i);
                        
                        arrInt.Add(i * 10 + 1);
                        arrInt.Add(i * 10 + 2);
                        arrInt.Add(i * 10 + 3);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Double;
                    p1.Value = arrDouble.ToArray();
                    cmd.Parameters.Add(p1);
                    
                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = arrString.ToArray();
                    cmd.Parameters.Add(p2);
             
                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int32;
                    p3.Value = arrInt.ToArray();
                    cmd.Parameters.Add(p3);

                    var rowsCount = cmd.ExecuteNonQuery();
                    Assert.AreEqual(Total * 3, rowsCount);

                    cmd.CommandText = $"SELECT * FROM {_testName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }
            }
        }

        [Test]
        public void TestPutArrayBind1()
        {
            CreateOrReplaceTable(_testName, new []
            {
                "cola INTEGER"
            });
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO {_testName} VALUES (?)";

                    const int Total = 70000;

                    var arrInt = new List<int>();
                    for (var i = 0; i < Total; i++)
                    {
                        arrInt.Add(i);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrInt.ToArray();
                    cmd.Parameters.Add(p1);

                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(70000, count);

                    cmd.CommandText = $"SELECT * FROM {_testName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }
            }
        }
    }
}
