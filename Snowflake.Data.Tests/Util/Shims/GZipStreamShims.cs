namespace System.IO.Compression;

public static class GZipStreamShims
{
    public static void Write(this GZipStream stream, ReadOnlySpan<byte> value)
    {
        stream.Write(value.ToArray(), 0, value.Length);
    }
}
