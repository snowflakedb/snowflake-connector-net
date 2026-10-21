using System;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.Mock;

class MockSecretDetector
{
    public static Mask MaskSecrets(string text)
    {
        var result = new Mask();
        try
        {
            throw new Exception("Test exception");
        }
        catch (Exception ex)
        {
            //We'll assume that the exception was raised during masking
            //to be safe consider that the log has sensitive information
            //and do not raise an exception.
            result.IsMasked = true;
            result.MaskedText = ex.Message;
            result.ErrStr = ex.Message;
        }
        return result;
    }
}
