using System;

namespace Snowflake.Data.Tests.Client
{
    public class AllNullableUnstructuredTypesClass
    {
        public string StringValue { get; set; }
        public char? CharValue { get; set; }
        public byte? ByteValue { get; set; }
        public sbyte? SByteValue { get; set; }
        public short? ShortValue { get; set; }
        public ushort? UShortValue { get; set; }
        public int? IntValue { get; set; }
        public int? UIntValue { get; set; }
        public long? LongValue { get; set; }
        public long? ULongValue { get; set; }
        public float? FloatValue { get; set; }
        public double? DoubleValue { get; set; }
        public decimal? DecimalValue { get; set; }
        public bool? BooleanValue { get; set; }
        public Guid? GuidValue { get; set; }
        public DateTime? DateTimeValue { get; set; }
        public DateTimeOffset? DateTimeOffsetValue { get; set; }
        public TimeSpan? TimeSpanValue { get; set; }
        public byte[] BinaryValue { get; set; }
        public string SemiStructuredValue { get; set; }
    }
}
