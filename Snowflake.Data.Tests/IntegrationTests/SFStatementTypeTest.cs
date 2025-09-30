namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using System.Data;
    using Snowflake.Data.Client;
    using System.Data.Common;

    [TestFixture]
    class SFStatementTypeTest : SFBaseTest
    {
        [Test]
        public void TestCallStatement()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand command = conn.CreateCommand())
                {
                    try
                    {
                        command.CommandText = "ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS=true";
                        command.ExecuteNonQuery();
                    }
                    catch (SnowflakeDbException ex)
                    {
                        // Expected error - SqlState: 22023, VendorCode: 1006
                        Assert.AreEqual("22023", ex.SqlState);
                    }

                    command.CommandText = "create or replace procedure\n"
                        + "TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant)\n"
                        + "returns string language javascript as $$\n"
                        + "let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]});\n"
                        + "res.next();\n"
                        + "return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2;\n"
                        + "$$;";
                    command.ExecuteNonQuery();

                    command.CommandText = "call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Double;
                    p1.Value = 1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = "[2,3]";
                    command.Parameters.Add(p2);

                    DbDataReader reader = command.ExecuteReader();

                    string result = "1 \"[2,3]\" [2,3]";
                    while (reader.Read())
                    {
                        Assert.AreEqual(result, reader.GetString(0));
                    }

                    command.CommandText = "drop procedure if exists TEST_SP_CALL_STMT_ENABLED(float, variant)";
                    command.ExecuteNonQuery();
                }
                conn.Close();
            }
        }
    }
}
