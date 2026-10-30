using Xunit;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using System;
using System.Linq;
using System.Text;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SecretDetectorTest
{
    private static Mask BasicMasking(string text)
    {
        var mask = SecretDetector.Instance.MaskSecrets(text);
        Assert.False(mask.IsMasked);
        Assert.Equal(text, mask.MaskedText);
        Assert.Null(mask.ErrStr);
        return mask;
    }

    [SFFact]
    public void TestNullString()
    {
        BasicMasking(null);
    }

    [SFFact]
    public void TestEmptyString()
    {
        BasicMasking("");
    }

    [SFFact]
    public void TestNoMasking()
    {
        BasicMasking("This string is innocuous");
    }

    [SFFact]
    public void TestExceptionInMasking()
    {
        var mask = MockSecretDetector.MaskSecrets("This string will raise an exception");
        Assert.True(mask.IsMasked);
        Assert.Equal("Test exception", mask.MaskedText);
        Assert.Equal("Test exception", mask.ErrStr);
    }

    private static void BasicMasking(string text, string expectedText)
    {
        var mask = SecretDetector.Instance.MaskSecrets(text);
        Assert.True(mask.IsMasked);
        Assert.Equal(expectedText, mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestAWSKeys()
    {
        // aws_key_id
        BasicMasking(@"aws_key_id='aaaaaaaa'", @"aws_key_id='****'");

        // aws_secret_key
        BasicMasking(@"aws_secret_key='aaaaaaaa'", @"aws_secret_key='****'");

        // access_key_id
        BasicMasking(@"access_key_id='aaaaaaaa'", @"access_key_id='****'");

        // secret_access_key
        BasicMasking(@"secret_access_key='aaaaaaaa'", @"secret_access_key='****'");

        // aws_key_id with colon
        BasicMasking(@"aws_key_id:'aaaaaaaa'", @"aws_key_id:'****'");

        // aws_key_id with single quote on key
        BasicMasking(@"'aws_key_id':'aaaaaaaa'", @"'aws_key_id':'****'");

        // aws_key_id with double quotes on key
        BasicMasking(@"""aws_key_id"":'aaaaaaaa'", @"""aws_key_id"":'****'");

        //If attribute is enclose in single or double quote
        BasicMasking(@"'aws_key_id'='aaaaaaaa'", @"'aws_key_id'='****'");
        BasicMasking(@"""aws_key_id""='aaaaaaaa'", @"""aws_key_id""='****'");

        //aws_key_id|aws_secret_key|access_key_id|secret_access_key)('|"")?(\s*[:|=]\s*)'([^']+)'
        // Delimiters before start of value to mask
        BasicMasking(@"aws_key_id:'aaaaaaaa'", @"aws_key_id:'****'");
        BasicMasking(@"aws_key_id='aaaaaaaa'", @"aws_key_id='****'");
    }

    [SFFact]
    public void TestAWSTokens()
    {
        // accessToken
        BasicMasking(@"accessToken"":""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"accessToken"":""XXXX""");

        // tempToken
        BasicMasking(@"tempToken"":""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"tempToken"":""XXXX""");

        // keySecret
        BasicMasking(@"keySecret"":""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"keySecret"":""XXXX""");

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"accessToken""  :  ""aB1aaaaaaaaaaZaaaaaaaaaaa9aaaaaaa=""", @"accessToken"":""XXXX""");
        BasicMasking(@"accessToken""  :  ""aB1aaaaaaaaaaZaaaaaaaa56aaaaaaaaaa==""", @"accessToken"":""XXXX""");
    }

    [SFFact]
    public void TestAWSServerSide()
    {
        // amz encryption
        BasicMasking(@"x-amz-server-side-encryption-customer-key:YtLf9S7iLprBMxSpP0Scm5MNgtsmK12hNd63wRpOGfI=",
            @"x-amz-server-side-encryption-customer-key:....");

        // amz encryption md5
        BasicMasking(@"x-amz-server-side-encryption-customer-key-md5:5SBvdH9fHaWsORVu7auB/A==",
            @"x-amz-server-side-encryption-customer-key-md5:....");

        // amz encryption algorithm
        BasicMasking(@"x-amz-server-side-encryption-customer-algorithm: ABC123",
            @"x-amz-server-side-encryption-customer-algorithm:....");

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"x-amz-server-side-encryptionthis-and-that: Scm5M=d/6_p-r5+/:j=8",
            @"x-amz-server-side-encryptionthis-and-that:....");
    }

    [SFFact]
    public void TestSASTokens()
    {
        // sig
        BasicMasking(@"sig=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"sig=****");

        // signature
        BasicMasking(@"signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"signature=****");

        // AWSAccessKeyId
        BasicMasking(@"AWSAccessKeyId=ABCDEFGHIJKL01234", @"AWSAccessKeyId=****"); // pragma: allowlist secret

        // password
        BasicMasking(@"password=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"password=****");

        // passcode
        BasicMasking(@"passcode=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"passcode=****");

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"sig=abCaa09aaa%%aaaaaaaaaa/aaaaa+aaaaa", @"sig=****");
    }

    [SFFact]
    public void TestSigV4QueryParams()
    {
        // X-Amz-Credential is masked by the SigV4 pattern.
        BasicMasking(
            @"X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260728%2Fus-east-1%2Fs3%2Faws4_request", // pragma: allowlist secret
            @"X-Amz-Credential=****");

        // X-Amz-Security-Token is masked (also covered by the token= pattern; the SigV4 rule adds parity).
        BasicMasking(
            @"X-Amz-Security-Token=IQoJb3JpZ2luX2VjEND%2F%2F%2F%2FwEaCXVzLWVhc3QtMQ", // pragma: allowlist secret
            @"X-Amz-Security-Token=****");
    }

    [SFFact]
    public void TestGcsQueryParams()
    {
        // X-Goog-Credential is masked by the GCS pattern.
        BasicMasking(
            @"X-Goog-Credential=GOOG1EEXAMPLEKEYID%2F20260728%2Fauto%2Fstorage%2Fgoog4_request", // pragma: allowlist secret
            @"X-Goog-Credential=****");

        // X-Goog-Signature is masked (via the signature= pattern; the GCS rule is a parity guard).
        BasicMasking(
            @"X-Goog-Signature=4a5b6c7d8e9f0011223344556677889900aabbccddeeff0011223344556677", // pragma: allowlist secret
            @"X-Goog-Signature=****");
    }

    [SFFact]
    public void TestPrivateKey()
    {
        // Verify that all allowed characters are correctly supported
        BasicMasking("-----BEGIN PRIVATE KEY-----\na0a==aaB/aa1aaaa\naaaaCaaa+aa95aaa\n-----END PRIVATE KEY-----", // pragma: allowlist secret
            "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----"); // pragma: allowlist secret
    }

    [SFFact]
    public void TestPrivateKeyProperty()
    {
        BasicMasking(@"something=anything;private_key=aaaaaa", @"something=anything;private_key=****");
        BasicMasking("something=anything;private_key \r\n  =aaaaaa", "something=anything;private_key \r\n  =****");
        BasicMasking(@"something=anything;private_key=aaaaaaaaaaaaaaaaaa", @"something=anything;private_key=****");
        BasicMasking(@"something=anything;private_key=a", @"something=anything;private_key=****");
        BasicMasking(@"something=anything;private_key=""a"";someOtherProperty=someValue", @"something=anything;private_key=****");
        BasicMasking(@"something=anything;private_key='a';someOtherProperty=someValue", @"something=anything;private_key=****");
        BasicMasking($"something=anything;private_key ={GetStringWithManyWeirdCharacters()}\r\nxxxxxx\r\nyyyyyy;someOtherProperty=someValue", @"something=anything;private_key =****");
    }

    private static string GetStringWithManyWeirdCharacters()
    {
        var bytes = new byte[256];
        for (var i = 0; i < 256; i++)
        {
            if (i < 20)
            {
                bytes[i] = 58;
            }
            bytes[i] = (byte)i;
        }
        return Encoding.Default.GetString(bytes);
    }

    [SFFact]
    public void TestPrivateKeyData()
    {
        BasicMasking(@"""privateKeyData"": ""aaaaaaaaaa""", @"""privateKeyData"": ""XXXX""");

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"""privateKeyData"": ""a/b+c=d0" + "\n" + "139\"", @"""privateKeyData"": ""XXXX""");
    }

    [SFFact]
    public void TestConnectionTokens()
    {
        // token
        BasicMasking(@"token:aaaaaaaa", @"token:****");

        // assertion content
        BasicMasking(@"assertion content:aaaaaaaa", @"assertion content:****");

        // Delimiters before start of value to mask
        BasicMasking(@"token""aaaaaaaa", @"token""****"); // "
        BasicMasking(@"token'aaaaaaaa", @"token'****"); // '
        BasicMasking(@"token=aaaaaaaa", @"token=****"); // =
        BasicMasking(@"token aaaaaaaa", @"token ****"); // {space}
        BasicMasking(@"token ="" 'aaaaaaaa", @"token =****"); // Mix

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"Token:a=b/c_d-e+F:025", @"Token:****");
    }

    [SFFact]
    public void TestPassword()
    {
        // password
        BasicMasking(@"password:aaaaaaaa", @"password:****");

        // proxypassword
        BasicMasking(@"proxypassword:aaaaaaaa", @"proxypassword:****");

        // pwd
        BasicMasking(@"pwd:aaaaaaaa", @"pwd:****");

        // passcode
        BasicMasking(@"passcode:aaaaaaaa", @"passcode:****");

        // client_secret
        BasicMasking(@"clientSecret:aaaaaaaa", @"clientSecret:****");
        BasicMasking(@"client_secret:aaaaaaaa", @"client_secret:****");
        BasicMasking(@"oauthClientSecret:aaaaaaaa", @"oauthClientSecret:****");

        // Delimiters before start of value to mask
        BasicMasking(@"password""aaaaaaaa", @"password""****"); // "
        BasicMasking(@"password'aaaaaaaa", @"password'****"); // '
        BasicMasking(@"password=aaaaaaaa", @"password=****"); // =
        BasicMasking(@"password aaaaaaaa", @"password ****"); // {space}
        BasicMasking(@"password ="" 'aaaaaaaa", @"password =****"); // Mix

        // Verify that all allowed characters are correctly supported
        BasicMasking(@"password:a!b""c#d$e%f&g'h(i)k*k+l,m;n<o=p>q?r@s[t]u^v_w`x{y|z}Az0123", @"password:****");
    }

    [SFFact]
    public void TestPasswordProperty()
    {
        BasicMasking(@"somethingBefore=cccc;password=aa", @"somethingBefore=cccc;password=****");
        BasicMasking(@"somethingBefore=cccc;password=aa;somethingNext=bbbb", @"somethingBefore=cccc;password=****");
        BasicMasking(@"somethingBefore=cccc;password=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;password=****");
        BasicMasking(@"somethingBefore=cccc;password=;somethingNext=bbbb", @"somethingBefore=cccc;password=****");
        BasicMasking(@"somethingBefore=cccc;password=", @"somethingBefore=cccc;password=****");
        BasicMasking(@"somethingBefore=cccc;password     =aa;somethingNext=bbbb", @"somethingBefore=cccc;password     =****");
        BasicMasking(@"somethingBefore=cccc;password="" 'aa", @"somethingBefore=cccc;password=****");

        BasicMasking(@"somethingBefore=cccc;proxypassword=aa", @"somethingBefore=cccc;proxypassword=****");
        BasicMasking(@"somethingBefore=cccc;proxypassword=aa;somethingNext=bbbb", @"somethingBefore=cccc;proxypassword=****");
        BasicMasking(@"somethingBefore=cccc;proxypassword=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;proxypassword=****");
        BasicMasking(@"somethingBefore=cccc;proxypassword=;somethingNext=bbbb", @"somethingBefore=cccc;proxypassword=****");
        BasicMasking(@"somethingBefore=cccc;proxypassword=", @"somethingBefore=cccc;proxypassword=****");
        BasicMasking(@"somethingBefore=cccc;proxypassword     =aa;somethingNext=bbbb", @"somethingBefore=cccc;proxypassword     =****");
        BasicMasking(@"somethingBefore=cccc;proxypassword="" 'aa", @"somethingBefore=cccc;proxypassword=****");

        BasicMasking(@"somethingBefore=cccc;private_key_pwd=aa", @"somethingBefore=cccc;private_key_pwd=****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd=aa;somethingNext=bbbb", @"somethingBefore=cccc;private_key_pwd=****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;private_key_pwd=****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd=;somethingNext=bbbb", @"somethingBefore=cccc;private_key_pwd=****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd=", @"somethingBefore=cccc;private_key_pwd=****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd     =aa;somethingNext=bbbb", @"somethingBefore=cccc;private_key_pwd     =****");
        BasicMasking(@"somethingBefore=cccc;private_key_pwd="" 'aa", @"somethingBefore=cccc;private_key_pwd=****");

        BasicMasking(@"somethingBefore=cccc;passcode=aa", @"somethingBefore=cccc;passcode=****");
        BasicMasking(@"somethingBefore=cccc;passcode=aa;somethingNext=bbbb", @"somethingBefore=cccc;passcode=****");
        BasicMasking(@"somethingBefore=cccc;passcode=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;passcode=****");
        BasicMasking(@"somethingBefore=cccc;passcode=;somethingNext=bbbb", @"somethingBefore=cccc;passcode=****");
        BasicMasking(@"somethingBefore=cccc;passcode=", @"somethingBefore=cccc;passcode=****");
        BasicMasking(@"somethingBefore=cccc;passcode     =aa;somethingNext=bbbb", @"somethingBefore=cccc;passcode     =****");
        BasicMasking(@"somethingBefore=cccc;passcode="" 'aa", @"somethingBefore=cccc;passcode=****");

        BasicMasking(@"somethingBefore=cccc;oauthClientSecret=aa", @"somethingBefore=cccc;oauthClientSecret=****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret=aa;somethingNext=bbbb", @"somethingBefore=cccc;oauthClientSecret=****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;oauthClientSecret=****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret=;somethingNext=bbbb", @"somethingBefore=cccc;oauthClientSecret=****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret=", @"somethingBefore=cccc;oauthClientSecret=****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret     =aa;somethingNext=bbbb", @"somethingBefore=cccc;oauthClientSecret     =****");
        BasicMasking(@"somethingBefore=cccc;oauthClientSecret="" 'aa", @"somethingBefore=cccc;oauthClientSecret=****");
    }

    [SFTheory]
    [InlineData("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID password was not given")]
    [InlineData("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID proxyPassword was not given")]
    [InlineData("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID private_key_pwd was not given")]
    public void TestPasswordFalsePositive(string falsePositiveMessage)
    {
        var mask = SecretDetector.Instance.MaskSecrets(falsePositiveMessage);
        Assert.False(mask.IsMasked);
        Assert.Equal(falsePositiveMessage, mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestMaskToken()
    {
        string longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U" + // pragma: allowlist secret
                           "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj" + // pragma: allowlist secret
                           "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F" + // pragma: allowlist secret
                           "mClCMBSissVsU3Ei590FP0lPQQhcSGcD" + // pragma: allowlist secret
                           "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU=" + // pragma: allowlist secret
                           "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj" + // pragma: allowlist secret
                           "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR" + // pragma: allowlist secret
                           "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5" + // pragma: allowlist secret
                           "XdJYuI8vhg=f0bKSq7AhQ2Bh"; // pragma: allowlist secret

        string tokenStrWithPrefix = "Token =" + longToken;
        var mask = SecretDetector.Instance.MaskSecrets(tokenStrWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"Token =****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string idTokenStrWithPrefix = "idToken : " + longToken;
        mask = SecretDetector.Instance.MaskSecrets(idTokenStrWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"idToken : ****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string sessionTokenStrWithPrefix = "sessionToken : " + longToken;
        mask = SecretDetector.Instance.MaskSecrets(sessionTokenStrWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"sessionToken : ****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string masterTokenStrWithPrefix = "masterToken : " + longToken;
        mask = SecretDetector.Instance.MaskSecrets(masterTokenStrWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"masterToken : ****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string assertionStrWithPrefix = "assertion content: " + longToken;
        mask = SecretDetector.Instance.MaskSecrets(assertionStrWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"assertion content: ****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string snowFlakeAuthToken = "Authorization: Snowflake Token=\"ver:1-hint:92019676298218-ETMsDgAAAXswwgJhABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAEF1tbNM3myWX6A9sNSK6rpIAAACA6StojDJS4q1Vi3ID+dtFEucCEvGMOte0eapK+reb39O6hTHYxLfOgSGsbvbM5grJ4dYdNJjrzDf1r07tID4I2RJJRYjS4/DWBJn98Untd3xeNnXE1/45HgvwKVHlmZQLVwfWAxI7ifl2MVDwJlcXBufLZoVMYhUd4np121d7zFwAFGQzKyzUYQwI3M9Nqja9syHgaotG\"";
        mask = SecretDetector.Instance.MaskSecrets(snowFlakeAuthToken);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"Authorization: Snowflake Token=****", mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestTokenFalsePositive()
    {
        string falsePositiveToken = "2020-04-30 23:06:04,069 - MainThread auth.py:397" +
                                    " - write_temporary_credential() - DEBUG - no ID " +
                                    "token is given when try to store temporary credential";

        var mask = SecretDetector.Instance.MaskSecrets(falsePositiveToken);
        Assert.False(mask.IsMasked);
        Assert.Equal(falsePositiveToken, mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestPasswords()
    {
        string randomPassword = "Fh[+2J~AcqeqW%?";

        string randomPasswordWithPrefix = "password:" + randomPassword;
        var mask = SecretDetector.Instance.MaskSecrets(randomPasswordWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"password:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomPasswordCaps = "PASSWORD:" + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomPasswordCaps);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"PASSWORD:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomPasswordMixCase = "PassWorD:" + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomPasswordMixCase);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"PassWorD:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomPasswordEqualSign = "password = " + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomPasswordEqualSign);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"password =****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomPwdWithPrefix = "pwd:" + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomPwdWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"pwd:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomClientSecretUppercaseWithPrefix = "CLIENT_SECRET:" + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomClientSecretUppercaseWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"CLIENT_SECRET:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        string randomOAuthClientSecretUppercaseWithPrefix = "OAUTHCLIENTSECRET:" + randomPassword;
        mask = SecretDetector.Instance.MaskSecrets(randomOAuthClientSecretUppercaseWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(@"OAUTHCLIENTSECRET:****", mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }


    [SFFact]
    public void TestTokenPassword()
    {
        string longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U" + // pragma: allowlist secret
                           "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj" + // pragma: allowlist secret
                           "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F" + // pragma: allowlist secret
                           "mClCMBSissVsU3Ei590FP0lPQQhcSGcD" + // pragma: allowlist secret
                           "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU=" + // pragma: allowlist secret
                           "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj" + // pragma: allowlist secret
                           "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR" + // pragma: allowlist secret
                           "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5" + // pragma: allowlist secret
                           "XdJYuI8vhg=f0bKSq7AhQ2Bh"; // pragma: allowlist secret

        string longToken2 = "ktL57KJemuq4-M+Q0pdRjCIMcf1mzcr" + // pragma: allowlist secret
                            "MwKteDS5DRE/Pb+5MzvWjDH7LFPV5b_" + // pragma: allowlist secret
                            "/tX/yoLG3b4TuC6Q5qNzsARPPn_zs/j" + // pragma: allowlist secret
                            "BbDOEg1-IfPpdsbwX6ETeEnhxkHIL4H" + // pragma: allowlist secret
                            "sP-V";

        string randomPwd = "Fh[+2J~AcqeqW%?";
        string randomPwd2 = randomPwd + "vdkav13";

        string testStringWithPrefix = "token=" + longToken +
                                      " random giberish " +
                                      "password:" + randomPwd;
        var mask = SecretDetector.Instance.MaskSecrets(testStringWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "token=****",
            mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // order reversed
        testStringWithPrefix = "password:" + randomPwd +
                               " random giberish " +
                               "token=" + longToken;
        mask = SecretDetector.Instance.MaskSecrets(testStringWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "password:****" +
            " random giberish " +
            "token=****",
            mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // multiple tokens and password
        testStringWithPrefix = "token=" + longToken +
                               " random giberish " +
                               "password:" + randomPwd +
                               " random giberish " +
                               "idToken:" + longToken2;
        mask = SecretDetector.Instance.MaskSecrets(testStringWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "token=****",
            mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // two passwords
        testStringWithPrefix = "password=" + randomPwd +
                               " random giberish " +
                               "pwd:" + randomPwd2;
        mask = SecretDetector.Instance.MaskSecrets(testStringWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "password=****", mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // multiple passwords
        testStringWithPrefix = "password=" + randomPwd +
                               " random giberish " +
                               "password=" + randomPwd2 +
                               " random giberish " +
                               "password=" + randomPwd;
        mask = SecretDetector.Instance.MaskSecrets(testStringWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "password=****",
            mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestTokenProperty()
    {
        BasicMasking(@"somethingBefore=cccc;token=aa", @"somethingBefore=cccc;token=****");
        BasicMasking(@"somethingBefore=cccc;token=aa;somethingNext=bbbb", @"somethingBefore=cccc;token=****");
        BasicMasking(@"somethingBefore=cccc;token=""aa"";somethingNext=bbbb", @"somethingBefore=cccc;token=****");
        BasicMasking(@"somethingBefore=cccc;token=;somethingNext=bbbb", @"somethingBefore=cccc;token=****");
        BasicMasking(@"somethingBefore=cccc;token=", @"somethingBefore=cccc;token=****");
        BasicMasking(@"somethingBefore=cccc;token     =aa;somethingNext=bbbb", @"somethingBefore=cccc;token     =****");
        BasicMasking(@"somethingBefore=cccc;token="" 'aa", @"somethingBefore=cccc;token=****");
    }

    [SFFact]
    public void TestCustomPattern()
    {
        string[] regex = new string[2]
        {
            @"(testCustomPattern\s*:\s*""([a-z]{8,})"")",
            @"(testCustomPattern\s*:\s*""([0-9]{8,})"")"
        };
        string[] masks = new string[2]
        {
            "maskCustomPattern1",
            "maskCustomPattern2"
        };

        SecretDetector.Instance.SetCustomPatterns(regex, masks);

        // Mask custom pattern
        string testString = "testCustomPattern: \"abcdefghijklmnop\"";
        var mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.True(mask.IsMasked);
        Assert.Equal(masks[0], mask.MaskedText);
        Assert.Null(mask.ErrStr);

        testString = "testCustomPattern: \"1234567890\"";
        mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.True(mask.IsMasked);
        Assert.Equal(masks[1], mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // Mask password and custom pattern
        testString = "password: abcdefghijklmnop testCustomPattern: \"abcdefghijklmnop\"";
        mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.True(mask.IsMasked);
        Assert.Equal("password: **** " + masks[0], mask.MaskedText);
        Assert.Null(mask.ErrStr);

        testString = "password: abcdefghijklmnop testCustomPattern: \"1234567890\"";
        mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.True(mask.IsMasked);
        Assert.Equal("password: **** " + masks[1], mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestCustomPatternClear()
    {
        string[] regex = new string[1] { @"(testCustomPattern\s*:\s*""([a-z]{8,})"")" };
        string[] masks = new string[1] { "maskCustomPattern1" };

        SecretDetector.Instance.SetCustomPatterns(regex, masks);

        // Mask custom pattern
        string testString = "testCustomPattern: \"abcdefghijklmnop\"";
        var mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.True(mask.IsMasked);
        Assert.Equal(masks[0], mask.MaskedText);
        Assert.Null(mask.ErrStr);

        // Clear custom patterns
        SecretDetector.Instance.ClearCustomPatterns();
        testString = "testCustomPattern: \"abcdefghijklmnop\"";
        mask = SecretDetector.Instance.MaskSecrets(testString);
        Assert.False(mask.IsMasked);
        Assert.Equal(testString, mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }

    [SFFact]
    public void TestCustomPatternUnequalCount()
    {
        string[] regex = new string[0];
        string[] masks = new string[1] { "maskCustomPattern1" };

        // Masks count is greater than regex
        try
        {
            SecretDetector.Instance.SetCustomPatterns(regex, masks);
        }
        catch (Exception ex)
        {
            Assert.Equal("Regex count and mask count must be equal.", ex.Message);
        }

        // Regex count is greater than masks
        regex = new string[2]
        {
            @"(testCustomPattern\s*:\s*""([0-9]{8,})"")",
            @"(testCustomPattern\s*:\s*""([0-9]{8,})"")"
        };
        try
        {
            SecretDetector.Instance.SetCustomPatterns(regex, masks);
        }
        catch (Exception ex)
        {
            Assert.Equal("Regex count and mask count must be equal.", ex.Message);
        }
    }

    [SFFact]
    public void TestHttpResponse()
    {
        string randomHttpResponse =
            "\"data\" : {" +
            "\"masterToken\" : \"ver:1-hint:92019676298218-ETMsDgAAAXrK7h+Y=" + // pragma: allowlist secret
            "\"token\" : \"_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U" + // pragma: allowlist secret
            "\"remMeValidityInSeconds\" : 0," +
            "\"healthCheckInterval\" : 12," +
            "\"newClientForUpgrade\" : null," +
            "\"sessionId\" : 1234";

        string randomHttpResponseWithPrefix = "Post response: " + randomHttpResponse;
        var mask = SecretDetector.Instance.MaskSecrets(randomHttpResponseWithPrefix);
        Assert.True(mask.IsMasked);
        Assert.Equal(
            "Post response: " +
            "\"data\" : {" +
            "\"masterToken\" : \"****" +
            "\"token\" : \"****" +
            "\"remMeValidityInSeconds\" : 0," +
            "\"healthCheckInterval\" : 12," +
            "\"newClientForUpgrade\" : null," +
            "\"sessionId\" : 1234",
            mask.MaskedText);
        Assert.Null(mask.ErrStr);
    }
}
