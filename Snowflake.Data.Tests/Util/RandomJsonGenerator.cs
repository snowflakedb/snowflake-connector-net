using System.IO;
using System.Text;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests
{
    public class RandomJsonGenerator
    {
        public static void GenerateRandomJsonFile(string fileName, int numberOfLines)
        {
            using (var fileStream = File.Create(fileName))
            {
                PopulateStreamWithRandomJson(numberOfLines, fileStream);
            }
        }

        public static string GenerateRandomJsonString(int numberOfLines)
        {
            var memoryStream = GenerateRandomJsonByteStream(numberOfLines);
            return Encoding.ASCII.GetString(memoryStream.ToArray());
        }

        private static MemoryStream GenerateRandomJsonByteStream(int numberOfLines)
        {
            var memoryStream = new MemoryStream();
            PopulateStreamWithRandomJson(numberOfLines, memoryStream);
            return memoryStream;
        }

        private static void PopulateStreamWithRandomJson(int numberOfLines, Stream outputStream)
        {
            WriteToStream("{\n", outputStream);
            for (var i = 0; i < numberOfLines; i++)
            {
                var line = GenerateRandomLine();
                var lineWithSeparators = (i == numberOfLines - 1) ? line + "\n" : line + ",\n";
                WriteToStream(lineWithSeparators, outputStream);
            }
            WriteToStream("}\n", outputStream);
        }

        private static void WriteToStream(string value, Stream outputStream)
        {
            var bytes = GetBytes(value);
            outputStream.Write(bytes, 0, bytes.Length);
        }

        private static byte[] GetBytes(string value) => Encoding.ASCII.GetBytes(value);

        private static string GenerateRandomLine()
        {
            return $"  \"{TestDataGenarator.NextAlphaNumeric(10)}\" : \"{TestDataGenarator.NextAlphaNumeric(256)}\"";
        }
    }
}
