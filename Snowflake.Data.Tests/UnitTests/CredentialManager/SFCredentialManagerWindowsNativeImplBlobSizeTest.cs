using System;
using System.Text;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    // SNOW-4199956 — proof-of-concept / reproduction (Windows only).
    //
    // Claim under test (from the ticket's source review):
    //   SFCredentialManagerWindowsNativeImpl.SaveCredentials() calls the CredWriteW
    //   P/Invoke but discards its bool return value and never calls
    //   Marshal.GetLastWin32Error(), even though the [DllImport] declares
    //   SetLastError = true. A native "false" return is not a .NET exception, so the
    //   surrounding try/catch does not catch it. Any write the OS rejects therefore
    //   fails SILENTLY: no exception, no error log, and on the next process start the
    //   token is simply gone.
    //
    // A concrete, documented trigger is the Windows Credential Manager blob-size
    // ceiling: CREDENTIALW.CredentialBlobSize "cannot be larger than
    // CRED_MAX_CREDENTIAL_BLOB_SIZE (5*512)" = 2560 bytes
    // (https://learn.microsoft.com/en-us/windows/win32/api/wincred/ns-wincred-credentialw).
    // The token is stored as UTF-16 (Encoding.Unicode, 2 bytes/char), so the blob
    // reaches the cap at 1280 characters; a 1281-character token is the first that
    // exceeds it. OAuth authorization-code tokens with a refresh_token scope commonly
    // exceed that length.
    //
    // These tests exercise the REAL Windows Credential Manager through the native
    // Advapi32 P/Invoke, so they only run on Windows (RunOnlyOnWindows) and are
    // skipped on Linux/macOS. They must NOT be forced onto other platforms: the
    // Advapi32 P/Invoke would throw DllNotFoundException there, which is a platform
    // artifact, not evidence about the blob-size limit.
    //
    // EXPECTED CI RESULT ON THE CURRENT (UNFIXED) DRIVER:
    //   - TestControl16CharTokenRoundTrips           -> PASS  (small blob, well under cap)
    //   - TestCredMaxBlobSizeTokenIsNotSilentlyLost  -> FAIL  (reproduces the defect:
    //                                                          no error surfaced AND the
    //                                                          token does not persist)
    // Once SaveCredentials checks CredWrite / GetLastWin32Error and handles the cap,
    // the second test turns green — it is written as the fix's acceptance test.
    public sealed class SFCredentialManagerWindowsNativeImplBlobSizeTest
    {
        // Microsoft-documented ceiling for CREDENTIALW.CredentialBlobSize, in bytes.
        private const int CredMaxCredentialBlobSizeBytes = 5 * 512; // 2560

        private readonly ISnowflakeCredentialManager _credentialManager =
            SFCredentialManagerWindowsNativeImpl.Instance;

        // Control: a short 16-char token must save and read back cleanly. This proves
        // the write path works when the blob is far below the OS cap, so a failure of
        // the oversized case below is attributable to size, not to the test harness.
        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestControl16CharTokenRoundTrips()
        {
            // arrange
            var key = "SNOW-4199956-control-small-token";
            var token = "access-token-123"; // 16 chars -> 32 bytes UTF-16, far below 2560
            var blobBytes = Encoding.Unicode.GetBytes(token).Length;

            try
            {
                // act
                var thrown = Record.Exception(() => _credentialManager.SaveCredentials(key, token));
                var readBack = _credentialManager.GetCredentials(key);

                // assert
                Assert.True(thrown == null,
                    $"SNOW-4199956 CONTROL: saving a {blobBytes}-byte token unexpectedly threw " +
                    $"{thrown?.GetType().Name}: {thrown?.Message}");
                Assert.Equal(token, readBack);
            }
            finally
            {
                _credentialManager.RemoveCredentials(key);
            }
        }

        // Experiment: a token whose UTF-16 blob exceeds CRED_MAX_CREDENTIAL_BLOB_SIZE.
        //
        // Contract a correct driver must honour: an over-cap write MUST NOT be silently
        // lost. SaveCredentials must EITHER surface the failure (throw / log the real
        // Win32 error) OR the credential must actually persist and read back. This
        // assertion fails in exactly one situation — the one the ticket describes —
        // where no error is surfaced AND the value is gone.
        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestCredMaxBlobSizeTokenIsNotSilentlyLost()
        {
            // arrange: 2048 chars -> 4096 bytes UTF-16, ~1.6x the 2560-byte cap.
            var key = "SNOW-4199956-oversized-token";
            var oversizedToken = new string('A', 2048);
            var blobBytes = Encoding.Unicode.GetBytes(oversizedToken).Length;
            Assert.True(blobBytes > CredMaxCredentialBlobSizeBytes,
                $"test setup error: blob {blobBytes} bytes must exceed cap {CredMaxCredentialBlobSizeBytes}");

            try
            {
                // act
                var thrown = Record.Exception(() => _credentialManager.SaveCredentials(key, oversizedToken));
                var readBack = _credentialManager.GetCredentials(key) ?? string.Empty;
                var roundTripped = string.Equals(readBack, oversizedToken, StringComparison.Ordinal);

                // A clearly greppable diagnostic line for the CI log.
                var diagnostic =
                    $"SNOW-4199956 REPRO: blob={blobBytes} bytes (cap {CredMaxCredentialBlobSizeBytes}); " +
                    $"SaveCredentials threw={(thrown == null ? "NONE" : thrown.GetType().Name + ": " + thrown.Message)}; " +
                    $"read-back length={readBack.Length} (expected {oversizedToken.Length}); " +
                    $"round-tripped={roundTripped}.";

                // assert: the defect is a swallowed error AND a lost value.
                Assert.True(thrown != null || roundTripped,
                    diagnostic + " Defect confirmed: the oversized write was neither surfaced as an " +
                    "error nor persisted. A correct driver must check CredWrite's return value, read " +
                    "Marshal.GetLastWin32Error() on failure, and either throw/log or persist the token.");
            }
            finally
            {
                _credentialManager.RemoveCredentials(key);
            }
        }
    }
}
