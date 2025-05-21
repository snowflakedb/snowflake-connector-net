using NUnit.Framework;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using System;
using System.Text;

namespace Snowflake.Data.Tests.UnitTests
{

    [TestFixture]
    class SecretDetectorTest
    {
        SecretDetector.Mask mask;

        [SetUp]
        public void BeforeTest()
        {
            mask = SecretDetector.MaskSecrets(null);
        }

        public void BasicMasking(string text)
        {
            mask = SecretDetector.MaskSecrets(text);
            Assert.IsFalse(mask.isMasked);
            Assert.AreEqual(text, mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
        public void TestNullString()
        {
            BasicMasking(null);
        }

        [Test]
        public void TestEmptyString()
        {
            BasicMasking("");
        }

        [Test]
        public void TestNoMasking()
        {
            BasicMasking("This string is innocuous");
        }

        [Test]
        public void TestExceptionInMasking()
        {
            mask = MockSecretDetector.MaskSecrets("This string will raise an exception");
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual("Test exception", mask.maskedText);
            Assert.AreEqual("Test exception", mask.errStr);
        }

        public void BasicMasking(string text, string expectedText)
        {
            mask = SecretDetector.MaskSecrets(text);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(expectedText, mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
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

        [Test]
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

        [Test]
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

        [Test]
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

        [Test]
        public void TestPrivateKey()
        {
            // Verify that all allowed characters are correctly supported
            BasicMasking("-----BEGIN PRIVATE KEY-----\na0a==aaB/aa1aaaa\naaaaCaaa+aa95aaa\n-----END PRIVATE KEY-----", // pragma: allowlist secret
                "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----"); // pragma: allowlist secret
        }

        [Test]
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

        private string GetStringWithManyWeirdCharacters()
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

        [Test]
        public void TestPrivateKeyData()
        {
            BasicMasking(@"""privateKeyData"": ""aaaaaaaaaa""", @"""privateKeyData"": ""XXXX""");

            // Verify that all allowed characters are correctly supported
            BasicMasking(@"""privateKeyData"": ""a/b+c=d0" + "\n" + "139\"", @"""privateKeyData"": ""XXXX""");
        }

        [Test]
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

        [Test]
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

        [Test]
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

        [Test]
        [TestCase("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID password was not given")]
        [TestCase("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID proxyPassword was not given")]
        [TestCase("2020-04-30 23:06:04,069 - MainThread auth.py:397 - write_temporary_credential() - DEBUG - no ID private_key_pwd was not given")]
        public void TestPasswordFalsePositive(string falsePositiveMessage)
        {
            mask = SecretDetector.MaskSecrets(falsePositiveMessage);
            Assert.IsFalse(mask.isMasked);
            Assert.AreEqual(falsePositiveMessage, mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
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
            mask = SecretDetector.MaskSecrets(tokenStrWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"Token =****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string idTokenStrWithPrefix = "idToken : " + longToken;
            mask = SecretDetector.MaskSecrets(idTokenStrWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"idToken : ****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string sessionTokenStrWithPrefix = "sessionToken : " + longToken;
            mask = SecretDetector.MaskSecrets(sessionTokenStrWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"sessionToken : ****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string masterTokenStrWithPrefix = "masterToken : " + longToken;
            mask = SecretDetector.MaskSecrets(masterTokenStrWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"masterToken : ****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string assertionStrWithPrefix = "assertion content: " + longToken;
            mask = SecretDetector.MaskSecrets(assertionStrWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"assertion content: ****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string snowFlakeAuthToken = "Authorization: Snowflake Token=\"ver:1-hint:92019676298218-ETMsDgAAAXswwgJhABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAEF1tbNM3myWX6A9sNSK6rpIAAACA6StojDJS4q1Vi3ID+dtFEucCEvGMOte0eapK+reb39O6hTHYxLfOgSGsbvbM5grJ4dYdNJjrzDf1r07tID4I2RJJRYjS4/DWBJn98Untd3xeNnXE1/45HgvwKVHlmZQLVwfWAxI7ifl2MVDwJlcXBufLZoVMYhUd4np121d7zFwAFGQzKyzUYQwI3M9Nqja9syHgaotG\"";
            mask = SecretDetector.MaskSecrets(snowFlakeAuthToken);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"Authorization: Snowflake Token=****", mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
        public void TestTokenFalsePositive()
        {
            string falsePositiveToken = "2020-04-30 23:06:04,069 - MainThread auth.py:397" +
                " - write_temporary_credential() - DEBUG - no ID " +
                "token is given when try to store temporary credential";

            mask = SecretDetector.MaskSecrets(falsePositiveToken);
            Assert.IsFalse(mask.isMasked);
            Assert.AreEqual(falsePositiveToken, mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
        public void TestPasswords()
        {
            string randomPassword = "Fh[+2J~AcqeqW%?";

            string randomPasswordWithPrefix = "password:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPasswordWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"password:****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomPasswordCaps = "PASSWORD:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPasswordCaps);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"PASSWORD:****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomPasswordMixCase = "PassWorD:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPasswordMixCase);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"PassWorD:****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomPasswordEqualSign = "password = " + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPasswordEqualSign);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"password =****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomPwdWithPrefix = "pwd:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPwdWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"pwd:****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomClientSecretUppercaseWithPrefix = "CLIENT_SECRET:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomClientSecretUppercaseWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"CLIENT_SECRET:****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomOAuthClientSecretUppercaseWithPrefix = "OAUTHCLIENTSECRET:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomOAuthClientSecretUppercaseWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"OAUTHCLIENTSECRET:****", mask.maskedText);
            Assert.IsNull(mask.errStr);
        }


        [Test]
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
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "token=****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);

            // order reversed
            testStringWithPrefix = "password:" + randomPwd +
               " random giberish " +
               "token=" + longToken;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "password:****" +
                " random giberish " +
                "token=****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);

            // multiple tokens and password
            testStringWithPrefix = "token=" + longToken +
                " random giberish " +
                "password:" + randomPwd +
                " random giberish " +
                "idToken:" + longToken2;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "token=****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);

            // two passwords
            testStringWithPrefix = "password=" + randomPwd +
                " random giberish " +
                "pwd:" + randomPwd2;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "password=****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            // multiple passwords
            testStringWithPrefix = "password=" + randomPwd +
                " random giberish " +
                "password=" + randomPwd2 +
                " random giberish " +
                "password=" + randomPwd;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "password=****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
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

        [Test]
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

            SecretDetector.SetCustomPatterns(regex, masks);

            // Mask custom pattern
            string testString = "testCustomPattern: \"abcdefghijklmnop\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(masks[0], mask.maskedText);
            Assert.IsNull(mask.errStr);

            testString = "testCustomPattern: \"1234567890\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(masks[1], mask.maskedText);
            Assert.IsNull(mask.errStr);

            // Mask password and custom pattern
            testString = "password: abcdefghijklmnop testCustomPattern: \"abcdefghijklmnop\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual("password: **** " + masks[0], mask.maskedText);
            Assert.IsNull(mask.errStr);

            testString = "password: abcdefghijklmnop testCustomPattern: \"1234567890\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual("password: **** " + masks[1], mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
        public void TestCustomPatternClear()
        {
            string[] regex = new string[1] { @"(testCustomPattern\s*:\s*""([a-z]{8,})"")" };
            string[] masks = new string[1] { "maskCustomPattern1" };

            SecretDetector.SetCustomPatterns(regex, masks);

            // Mask custom pattern
            string testString = "testCustomPattern: \"abcdefghijklmnop\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(masks[0], mask.maskedText);
            Assert.IsNull(mask.errStr);

            // Clear custom patterns
            SecretDetector.ClearCustomPatterns();
            testString = "testCustomPattern: \"abcdefghijklmnop\"";
            mask = SecretDetector.MaskSecrets(testString);
            Assert.IsFalse(mask.isMasked);
            Assert.AreEqual(testString, mask.maskedText);
            Assert.IsNull(mask.errStr);
        }

        [Test]
        public void TestCustomPatternUnequalCount()
        {
            string[] regex = new string[0];
            string[] masks = new string[1] { "maskCustomPattern1" };

            // Masks count is greater than regex
            try
            {
                SecretDetector.SetCustomPatterns(regex, masks);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("Regex count and mask count must be equal.", ex.Message);
            }

            // Regex count is greater than masks
            regex = new string[2]
            {
                @"(testCustomPattern\s*:\s*""([0-9]{8,})"")",
                @"(testCustomPattern\s*:\s*""([0-9]{8,})"")"
            };
            try
            {
                SecretDetector.SetCustomPatterns(regex, masks);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("Regex count and mask count must be equal.", ex.Message);
            }
        }

        [Test]
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
            mask = SecretDetector.MaskSecrets(randomHttpResponseWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "Post response: " +
                "\"data\" : {" +
                "\"masterToken\" : \"****" +
                "\"token\" : \"****" +
                "\"remMeValidityInSeconds\" : 0," +
                "\"healthCheckInterval\" : 12," +
                "\"newClientForUpgrade\" : null," +
                "\"sessionId\" : 1234",
                mask.maskedText);
            Assert.IsNull(mask.errStr);
        }
    }
}
