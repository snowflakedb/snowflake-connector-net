/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using Newtonsoft.Json;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System.Text;
    using System.Globalization;
    using System.Collections.Generic;

    [TestFixture]    
    class SFBindTestIT : SFBaseTest
    {
        [Test]
        public void testArrayBind()
        {
            
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                
                CreateOrReplaceTable(conn, TableName, new []
                {
                    "cola INTEGER",
                    "colb STRING"
                });

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {TableName} values (?, ?)";
                    cmd.CommandText = insertCommand;

                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = new int[] { 1, 2, 3 };
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = new string[] { "str1", "str2", "str3" };
                    cmd.Parameters.Add(p2);

                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(3, count);
                }

                conn.Close();
            }
        }

        [Test]
        public void testBindNullValue()
        {
            using (SnowflakeDbConnection dbConnection = new SnowflakeDbConnection())
            {
                dbConnection.ConnectionString = ConnectionString;
                dbConnection.Open();
                CreateOrReplaceTable(dbConnection, TableName, new[]
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
                foreach (DbType type in Enum.GetValues(typeof(DbType)))
                {
                    bool isTypeSupported = true;
                    string colName = null;
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        var param = command.CreateParameter();
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
                            command.CommandText = $"insert into {TableName}({colName}) values(:p0)";
                            param.Value = DBNull.Value;
                            command.Parameters.Add(param);
                            int rowsInserted = command.ExecuteNonQuery();
                            Assert.AreEqual(1, rowsInserted);
                        }
                        else
                        {
                            try
                            {
                                command.CommandText = $"insert into {TableName}(stringData) values(:p0)";
                                param.Value = DBNull.Value;
                                command.Parameters.Add(param);
                                int rowsInserted = command.ExecuteNonQuery();
                            }
                            catch (SnowflakeDbException e)
                            {
                                Assert.AreEqual(270053, e.ErrorCode);
                            }
                        }
                    }

                    if (isTypeSupported)
                    {
                        using (IDbCommand command = dbConnection.CreateCommand())
                        {
                            command.CommandText = $"select {colName} from {TableName};";
                            using (IDataReader reader = command.ExecuteReader())
                            {
                                reader.Read();
                                Assert.IsTrue(reader.IsDBNull(0));
                                reader.Close();
                            }
                        }
                    }

                    // Clean up between each case
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = $"DELETE FROM {TableName}";
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        [Test]
        public void testBindValue()
        {
            using (SnowflakeDbConnection dbConnection = new SnowflakeDbConnection())
            {
                dbConnection.ConnectionString = ConnectionString;
                dbConnection.Open();
                
                CreateOrReplaceTable(dbConnection, TableName, new[]
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
                
                foreach (DbType type in Enum.GetValues(typeof(DbType)))
                {
                    bool isTypeSupported = true;
                    string colName = null;
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        var param = command.CreateParameter();
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
                            command.CommandText = $"insert into {TableName}({colName}) values(:p0)";
                            command.Parameters.Add(param);
                            int rowsInserted = command.ExecuteNonQuery();
                            Assert.AreEqual(1, rowsInserted);
                        }
                        else
                        {
                            try
                            {
                                command.CommandText = $"insert into {TableName}(stringData) values(:p0)";
                                param.Value = DBNull.Value;
                                command.Parameters.Add(param);
                                int rowsInserted = command.ExecuteNonQuery();
                            }
                            catch (SnowflakeDbException e)
                            {
                                Assert.AreEqual(270053, e.ErrorCode);
                            }
                        }
                    }

                    if (isTypeSupported)
                    {
                        using (IDbCommand command = dbConnection.CreateCommand())
                        {
                            command.CommandText = $"select {colName} from {TableName};";
                            using (IDataReader reader = command.ExecuteReader())
                            {
                                reader.Read();
                                Assert.IsTrue(!reader.IsDBNull(0));
                                reader.Close();
                            }
                        }
                    }

                    // Clean up between each case
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = $"DELETE FROM {TableName}";
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        [Test]
        public void testBindValueWithSFDataType()
        {
            using (SnowflakeDbConnection dbConnection = new SnowflakeDbConnection())
            {
                dbConnection.ConnectionString = ConnectionString;
                dbConnection.Open();
                foreach (SFDataType type in Enum.GetValues(typeof(SFDataType)))
                {
                    if (!type.Equals(SFDataType.None))
                    {
                        bool isTypeSupported = true;
                        string[] columns;
                        if (!type.Equals(SFDataType.FIXED))
                        {
                            columns = new[]
                            {
                                $"data {type.ToString()}",
                                "unsupportedType VARCHAR"
                            };
                        }
                        else
                        {
                            columns = new[]
                            {
                                "data NUMBER",
                                "unsupportedType VARCHAR"
                            };
                        }
                        
                        CreateOrReplaceTable(dbConnection, TableName, columns);

                        using (IDbCommand command = dbConnection.CreateCommand())
                        {
                            SnowflakeDbParameter param = (SnowflakeDbParameter)command.CreateParameter();
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
                                command.CommandText = $"insert into {TableName}(data) values(:p0)";
                                command.Parameters.Add(param);
                                int rowsInserted = command.ExecuteNonQuery();
                                Assert.AreEqual(1, rowsInserted);
                            }
                            // DB rejects query if param type is VARIANT, OBJECT or ARRAY
                            else if (!type.Equals(SFDataType.VARIANT) && 
                                     !type.Equals(SFDataType.OBJECT) &&
                                     !type.Equals(SFDataType.ARRAY))
                            {
                                try
                                {
                                    command.CommandText = $"insert into {TableName}(unsupportedType) values(:p0)";
                                    param.Value = DBNull.Value;
                                    command.Parameters.Add(param);
                                    int rowsInserted = command.ExecuteNonQuery();
                                }
                                catch (SnowflakeDbException e)
                                {
                                    Assert.AreEqual(270054, e.ErrorCode);
                                }
                            }
                        }

                        if (isTypeSupported)
                        {
                            using (IDbCommand command = dbConnection.CreateCommand())
                            {
                                command.CommandText = $"select data from {TableName};";
                                using (IDataReader reader = command.ExecuteReader())
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
        public void testParameterCollection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
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

                    Array parameters = Array.CreateInstance(typeof(IDbDataParameter), 3);
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

                conn.Close();
            }
        }

        [Test]
        public void testPutArrayBind()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                
                CreateOrReplaceTable(conn, TableName, new []
                {
                    "cola INTEGER",
                    "colb STRING",
                    "colc DATE",
                    "cold TIME",
                    "cole TIMESTAMP_NTZ",
                    "colf TIMESTAMP_TZ" 
                });

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {TableName} values (?, ?, ?, ?, ?, ?)";
                    cmd.CommandText = insertCommand;

                    int total = 250000;

                    List<int> arrint = new List<int>();
                    for (int i=0; i<total; i++)
                    {
                        arrint.Add(i * 10 + 1);
                        arrint.Add(i * 10 + 2);
                        arrint.Add(i * 10 + 3);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrint.ToArray();
                    cmd.Parameters.Add(p1);

                    List<string> arrstring = new List<string>();
                    for (int i=0; i<total; i++)
                    {
                        arrstring.Add("str1");
                        arrstring.Add("str2");
                        arrstring.Add("str3");
                    }
                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = arrstring.ToArray();
                    cmd.Parameters.Add(p2);
                    
                    DateTime date1 = DateTime.ParseExact("2000-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime date2 = DateTime.ParseExact("2020-05-11 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime date3 = DateTime.ParseExact("2021-07-22 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    List<DateTime> arrDate = new List<DateTime>();
                    for (int i=0; i<total; i++)
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

                    DateTime time1 = DateTime.ParseExact("00:00:00.0000000", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime time2 = DateTime.ParseExact("23:59:59.9999999", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime time3 = DateTime.ParseExact("12:35:41.3333333", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    List<DateTime> arrTime = new List<DateTime>();
                    for (int i = 0; i < total; i++)
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

                    DateTime ntz1 = DateTime.ParseExact("2017-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    DateTime ntz2 = DateTime.ParseExact("2020-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    DateTime ntz3 = DateTime.ParseExact("2022-04-01 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    List<DateTime> arrNtz = new List<DateTime>();
                    for (int i = 0; i < total; i++)
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

                    DateTimeOffset tz1 = DateTimeOffset.Now;
                    DateTimeOffset tz2 = DateTimeOffset.UtcNow;
                    DateTimeOffset tz3 = new DateTimeOffset(2007, 1, 1, 12, 0, 0, new TimeSpan(4, 0, 0));
                    List<DateTimeOffset> arrTz = new List<DateTimeOffset>();
                    for (int i = 0; i < total; i++)
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
                    Assert.AreEqual(total * 3, count);

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                    
                    //cmd.CommandText = "drop table if exists testPutArrayBind";
                    //cmd.ExecuteNonQuery();
                    
                }

                conn.Close();
            }
        }
        
        [Test]
        public void testPutArrayBindWorkDespiteOtTypeNameHandlingAuto()
        {
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings {
                TypeNameHandling = TypeNameHandling.Auto
            };
            
            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                
                CreateOrReplaceTable(conn, TableName, new []
                {
                    "cola REAL",
                    "colb TEXT",
                    "colc NUMBER(38,0)"
                });

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    var insertCommand = $"insert into {TableName} values (?, ?, ?)";
                    cmd.CommandText = insertCommand;

                    var total = 250;
                    
                    List<double> arrdouble = new List<double>();
                    List<string> arrstring = new List<string>();
                    List<int> arrint = new List<int>();
                    for (int i=0; i<total; i++)
                    {
                        arrdouble.Add(i * 10 + 1);
                        arrdouble.Add(i * 10 + 2);
                        arrdouble.Add(i * 10 + 3);
                        
                        arrstring.Add("stra"+i);
                        arrstring.Add("strb"+i);
                        arrstring.Add("strc"+i);
                        
                        arrint.Add(i * 10 + 1);
                        arrint.Add(i * 10 + 2);
                        arrint.Add(i * 10 + 3);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Double;
                    p1.Value = arrdouble.ToArray();
                    cmd.Parameters.Add(p1);
                    
                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = arrstring.ToArray();
                    cmd.Parameters.Add(p2);
             
                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int32;
                    p3.Value = arrint.ToArray();
                    cmd.Parameters.Add(p3);

                    var rowsCount = cmd.ExecuteNonQuery();
                    Assert.AreEqual(total * 3, rowsCount);

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }

                conn.Close();
            }
        }

        [Test]
        public void testPutArrayBind1()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                
                CreateOrReplaceTable(conn, TableName, new []
                {
                    "cola INTEGER"
                });

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {TableName} values (?)";
                    cmd.CommandText = insertCommand;

                    int total = 70000;

                    List<int> arrint = new List<int>();
                    for (int i = 0; i < total; i++)
                    {
                        arrint.Add(i);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrint.ToArray();
                    cmd.Parameters.Add(p1);

                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(70000, count);

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }
                conn.Close();
            }
        }
    }
}
