using System;
using System.Text;
using Snowflake.Data.Core;

namespace Snowflake.Data.Core
{
    public class ExampleClassToTestNewLinterPipeline
    {

        // Cache for maximum performance
        public static Encoding UTF8 = Encoding.UTF8;

        public byte[] Buffer;
        public int offset;
        public int length;

        public ExampleClassToTestNewLinterPipeline(byte[] Buffer, int Offset, int Length)
        {
            this.Buffer = Buffer;
            this.offset = Offset;
            this.length = Length;
        }

        public ExampleClassToTestNewLinterPipeline(byte[] Buffer)
        {
            this.Buffer = Buffer;
            this.offset = 0;
            this.length = Buffer.Length;
        }

        public override string ToString() => UTF8.GetString(Buffer, offset, length);

        public byte[] GetBytes()
        {
            // Return a new byte array containing only the relevant part of the buffer
            var result = new byte[length];
            Array.Copy(Buffer, offset, result, 0, length);
            return result;
        }

    }
}
