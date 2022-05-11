/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using NUnit.Framework;

    [TestFixture]
    class SFDelCharT : SFBaseTest
    {

        [Test]
        public void testDelChar()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                
                IDbCommand cmd = conn.CreateCommand();
                
                cmd.CommandText = "create or replace table deltest (col string)";
                int res = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, res);

                string insertCommand = "insert into deltest(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => 35000)))";
                cmd.CommandText = insertCommand;
                IDataReader insertReader = cmd.ExecuteReader();
                Assert.AreEqual(35000, insertReader.RecordsAffected);
                
                string selectCommand = "select * from deltest";
                cmd.CommandText = selectCommand;
                cmd.CommandType = System.Data.CommandType.Text;
                
                var sb = new StringBuilder();
                var count = 0;
                using (var sw = new System.IO.StreamWriter(@"test.txt", true, Encoding.UTF8, 4096))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            count++;
                            var obj = new object[reader.FieldCount];
                            reader.GetValues(obj);
                            var val = obj[0] ?? String.Empty;
                            if (val.ToString().Contains("u007f"))
                            {
                                Console.WriteLine("Contains del Garbled characters!");
                            }
                            sb.Append(String.Join("\t", obj));
                            sb.Append("\r\n");
                            if (count > 500)
                            {
                                sw.Write(sb.ToString());
                                sw.Flush();
                                sb.Clear();
                                count = 0;
                            }
                        }
                    }
                    sw.Write(sb.ToString());
                    sw.Flush();
                    sb.Clear();
                }

                cmd.CommandText = "drop table if exists deltest";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
            }
        }

        [Test]
        public void testDelChar1()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                
                cmd.CommandText = "create or replace table deltest1 (col string)";
                int res = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, res);

                string insertCommand = "insert into deltest1(select hex_decode_string(hex_encode('snow') || '7F' || hex_encode('FLAKE')) from table(generator(rowcount => 4000)))";
                cmd.CommandText = insertCommand;
                IDataReader insertReader = cmd.ExecuteReader();
                Assert.AreEqual(4000, insertReader.RecordsAffected);
                
                string selectCommand = "select * from deltest1";
                cmd.CommandText = selectCommand;
                cmd.CommandType = System.Data.CommandType.Text;

                var sb = new StringBuilder();
                var count = 0;
                using (var sw = new System.IO.StreamWriter(@"test.txt", true, Encoding.UTF8, 4096))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            count++;
                            var obj = new object[reader.FieldCount];
                            reader.GetValues(obj);
                            var val = obj[0] ?? String.Empty;
                            if (val.ToString().Contains("u007f"))
                            {
                                Console.WriteLine("Contains del Garbled characters!");
                            }
                            sb.Append(String.Join("\t", obj));
                            sb.Append("\r\n");
                            if (count > 500)
                            {
                                sw.Write(sb.ToString());
                                sw.Flush();
                                sb.Clear();
                                count = 0;
                            }
                        }
                    }
                    sw.Write(sb.ToString());
                    sw.Flush();
                    sb.Clear();
                }

                cmd.CommandText = "drop table if exists deltest";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
            }
        }
    }
}
