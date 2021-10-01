using System;
using System.IO;
using Snowflake.Data.Client;

namespace ManualSmokeTestForQueryId
{
    class Program
    {
        static void Main(string[] args)
        {
            var conn = CreateSnowflakeConnection();
            using SnowflakeDbCommand cmd = (SnowflakeDbCommand) conn.CreateCommand();
            cmd.CommandText = "Select 'Please return my queryId!;'";
            SnowflakeDbDataReader reader = (SnowflakeDbDataReader) cmd.ExecuteReader();
            reader.Read();
            
            // prints 'Result: Please return my queryId!;'
            Console.WriteLine($@"Result: {reader.GetString(0)}");
            
            // prints e.g. 'QueryId: 019f50d7-3200-a565-0000-52cd1034d5fa'
            Console.WriteLine($@"QueryId: {reader.GetQueryId()}");
        }

        private static SnowflakeDbConnection CreateSnowflakeConnection()
        {
            SnowflakeDbConnection conn = new SnowflakeDbConnection();
            conn.ConnectionString = File.ReadAllText(
                Path.Combine(
                    Directory.GetCurrentDirectory(),
                    "connection_string.txt"
                )
            );
            conn.Open();
            return conn;
        }
    }
}