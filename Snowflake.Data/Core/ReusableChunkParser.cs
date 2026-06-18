using System.IO;
using System.Text;

namespace Snowflake.Data.Core
{
    using Snowflake.Data.Client;
    using System.Threading.Tasks;

    internal class FastStreamWrapper
    {
        Stream wrappedStream;
        byte[] buffer = new byte[32768];
        int count = 0;
        int next = 0;

        public FastStreamWrapper(Stream s)
        {
            wrappedStream = s;
        }

        // Small method to encourage inlining
        public int ReadByte()
        {
            // fast path first
            if (next < count)
                return buffer[next++];
            else
                return ReadByteSlow();
        }

        private int ReadByteSlow()
        {
            // fast path first
            if (next < count)
                return buffer[next++];

            if (count >= 0)
            {
                next = 0;
                count = wrappedStream.Read(buffer, 0, buffer.Length);
            }

            if (count <= 0)
            {
                count = -1;
                return -1;
            }

            return buffer[next++];
        }
    }

    internal class ReusableChunkParser : IChunkParser
    {
        // Very fast parser, only supports strings and nulls
        // Never generates parsing errors

        private readonly Stream stream;

        internal ReusableChunkParser(Stream stream)
        {
            this.stream = stream;
        }

        public async Task ParseChunk(IResultChunk chunk)
        {
            SFReusableChunk rc = (SFReusableChunk)chunk;

            bool inString = false;
            int c;
            var input = new FastStreamWrapper(stream);
            var ms = new FastMemoryStream();
            await Task.Run(() =>
            {
                while ((c = input.ReadByte()) >= 0)
                {
                    if (!inString)
                    {
                        // n means null
                        // " quote means begin string
                        // all else are ignored
                        if (c == '"')
                        {
                            inString = true;
                        }
                        else if (c == 'n')
                        {
                            rc.AddCell(null, 0);
                        }
                        // ignore anything else
                    }
                    else
                    {
                        // Inside a string, look for end string
                        // Anything else is saved in the buffer
                        if (c == '"')
                        {
                            rc.AddCell(ms.GetBuffer(), ms.Length);
                            ms.Clear();
                            inString = false;
                        }
                        else if (c == '\\')
                        {
                            bool caseU = false;
                            // Process next character
                            c = input.ReadByte();
                            switch (c)
                            {
                                case 'n':
                                    c = '\n';
                                    break;
                                case 'r':
                                    c = '\r';
                                    break;
                                case 'b':
                                    c = '\b';
                                    break;
                                case 't':
                                    c = '\t';
                                    break;
                                case 'u':
                                    caseU = true;
                                    StringBuilder byteStr = new StringBuilder("");
                                    for (int i = 0; i < 4; i++)
                                    {
                                        byteStr.Append((char)input.ReadByte());
                                    }
                                    int ascii = int.Parse(byteStr.ToString(), System.Globalization.NumberStyles.HexNumber);
                                    char asciiChar = (char)ascii;
                                    ms.WriteByte((byte)asciiChar);
                                    break;
                                case -1:
                                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected end of stream in escape sequence");
                            }
                            // The 'u' case already writes to stream so skip to prevent re-writing
                            // If not skipped, unicode characters are added an extra u (e.g "/u007f" becomes "/u007fu")
                            if (!caseU)
                            {
                                ms.WriteByte((byte)c);
                            }
                        }
                        else
                        {
                            ms.WriteByte((byte)c);
                        }
                    }
                }
                if (inString)
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR, $"Unexpected end of stream in string");
            });
        }
    }
}
