namespace Snowflake.Data.Core.CredentialManager
{
    internal enum TokenType
    {
        [StringAttr(value = "ID_TOKEN")]
        IdToken,
        [StringAttr(value = "MFA_TOKEN")]
        MFAToken
    }
}
