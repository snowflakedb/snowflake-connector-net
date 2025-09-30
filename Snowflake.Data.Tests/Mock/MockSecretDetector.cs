using System;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.Mock
{
    class MockSecretDetector
    {
        public static SecretDetector.Mask MaskSecrets(string text)
        {
            SecretDetector.Mask result = new SecretDetector.Mask();
            try
            {
                throw new Exception("Test exception");
            }
            catch (Exception ex)
            {
                //We'll assume that the exception was raised during masking
                //to be safe consider that the log has sensitive information
                //and do not raise an exception.
                result.isMasked = true;
                result.maskedText = ex.Message;
                result.errStr = ex.Message;
            }
            return result;
        }
    }
}
