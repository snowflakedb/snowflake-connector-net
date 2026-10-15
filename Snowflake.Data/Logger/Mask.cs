namespace Snowflake.Data.Log;

internal ref struct Mask
{
    internal bool IsMasked;
    internal string MaskedText;
    internal string ErrStr;

    public Mask(bool isMasked = false, string maskedText = null, string errStr = null)
    {
        IsMasked = isMasked;
        MaskedText = maskedText;
        ErrStr = errStr;
    }
}
