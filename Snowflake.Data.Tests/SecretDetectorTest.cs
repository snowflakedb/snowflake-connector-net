/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Log;
    using Snowflake.Data.Tests.Mock;

    [TestFixture]
    class SecretDetectorTest : SFBaseTest
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
        }

        [Test]
        public void TestAWSTokens()
        {
            // accessToken
            BasicMasking(@"accessToken:""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"accessToken"":""XXXX""");

            // tempToken
            BasicMasking(@"tempToken:""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"tempToken"":""XXXX""");

            // keySecret
            BasicMasking(@"keySecret:""aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""", @"keySecret"":""XXXX""");
        }

        [Test]
        public void TestSASTokens()
        {
            // sig
            BasicMasking(@"sig=?P<secret>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"sig=****");

            // signature
            BasicMasking(@"signature=?P<secret>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"signature=****");

            // AWSAccessKeyId
            BasicMasking(@"AWSAccessKeyId=?P<secret>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"AWSAccessKeyId=****");

            // password
            BasicMasking(@"password=?P<secret>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"password=****");

            // passcode
            BasicMasking(@"passcode=?P<secret>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", @"passcode=****");
        }

        [Test]
        public void TestPrivateKey()
        {
            BasicMasking("-----BEGIN PRIVATE KEY-----\naaaaaaaaaaaaaaaa\naaaaaaaaaaaaaaaa\n-----END PRIVATE KEY-----",
                "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----");
        }

        [Test]
        public void TestPrivateKeyData()
        {
            BasicMasking(@"""privateKeyData"": ""aaaaaaaaaa""", @"""privateKeyData"": ""XXXX""");
        }

        [Test]
        public void TestConnectionTokens()
        {
            // token
            BasicMasking(@"token:aaaaaaaa", @"token:****");

            // assertion content
            BasicMasking(@"assertion content:aaaaaaaa", @"assertion content:****");
        }

        [Test]
        public void TestPassword()
        {
            // password
            BasicMasking(@"password:aaaaaaaa", @"password:****");

            // pwd
            BasicMasking(@"pwd:aaaaaaaa", @"pwd:****");
        }

        [Test]
        public void TestMaskToken()
        {
            string longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U" +
                 "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj" +
                 "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F" +
                 "mClCMBSissVsU3Ei590FP0lPQQhcSGcD" +
                 "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU=" +
                 "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj" +
                 "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR" +
                 "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5" +
                 "XdJYuI8vhg=f0bKSq7AhQ2Bh";

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
            Assert.AreEqual(@"password = ****", mask.maskedText);
            Assert.IsNull(mask.errStr);

            string randomPwdWithPrefix = "pwd:" + randomPassword;
            mask = SecretDetector.MaskSecrets(randomPwdWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(@"pwd:****", mask.maskedText);
            Assert.IsNull(mask.errStr);
        }


        [Test]
        public void TestTokenPassword()
        {
            string longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U" +
                 "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj" +
                 "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F" +
                 "mClCMBSissVsU3Ei590FP0lPQQhcSGcD" +
                 "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU=" +
                 "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj" +
                 "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR" +
                 "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5" +
                 "XdJYuI8vhg=f0bKSq7AhQ2Bh";

            string longToken2 = "ktL57KJemuq4-M+Q0pdRjCIMcf1mzcr" +
                  "MwKteDS5DRE/Pb+5MzvWjDH7LFPV5b_" +
                  "/tX/yoLG3b4TuC6Q5qNzsARPPn_zs/j" +
                  "BbDOEg1-IfPpdsbwX6ETeEnhxkHIL4H" +
                  "sP-V";

            string randomPwd = "Fh[+2J~AcqeqW%?";
            string randomPwd2 = randomPwd + "vdkav13";

            string testStringWithPrefix = "token=" + longToken +
                           " random giberish " +
                           "password:" + randomPwd;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "token=****" +
                " random giberish " +
                "password:****", 
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
                "token=****" +
                " random giberish " +
                "password:****" +
                " random giberish " +
                "idToken:****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);

            // two passwords
            testStringWithPrefix = "password=" + randomPwd +
                " random giberish " +
                "pwd:" + randomPwd2;
            mask = SecretDetector.MaskSecrets(testStringWithPrefix);
            Assert.IsTrue(mask.isMasked);
            Assert.AreEqual(
                "password=****" +
                " random giberish " +
                "pwd:****",
                mask.maskedText);
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
                "password=****" +
                " random giberish " +
                "password=****" +
                " random giberish " +
                "password=****",
                mask.maskedText);
            Assert.IsNull(mask.errStr);
        }
    }
}