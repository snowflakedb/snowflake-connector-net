namespace Snowflake.Data.Log;

internal interface ISecretDetector
{
    void SetCustomPatterns(string[] customRegex, string[] customMask);
    void ClearCustomPatterns();
    Mask MaskSecrets(string text);
}
