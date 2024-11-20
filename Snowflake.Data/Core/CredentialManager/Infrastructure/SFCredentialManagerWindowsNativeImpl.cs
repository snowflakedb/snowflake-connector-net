/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Win32.SafeHandles;
using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{

    internal class SFCredentialManagerWindowsNativeImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerWindowsNativeImpl>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public static readonly SFCredentialManagerWindowsNativeImpl Instance = new SFCredentialManagerWindowsNativeImpl();

        public string GetCredentials(string key)
        {
            try
            {
                _lock.EnterReadLock();
                s_logger.Debug($"Getting the credentials for key: {key}");
                IntPtr nCredPtr;
                if (!CredRead(key, 1 /* Generic */, 0, out nCredPtr))
                {
                    s_logger.Info($"Unable to get credentials for key: {key}");
                    return "";
                }

                using (var critCred = new CriticalCredentialHandle(nCredPtr))
                {
                    var cred = critCred.GetCredential();
                    return cred.CredentialBlob;
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void RemoveCredentials(string key)
        {
            try
            {
                _lock.EnterWriteLock();
                s_logger.Debug($"Removing the credentials for key: {key}");

                if (!CredDelete(key, 1 /* Generic */, 0))
                {
                    s_logger.Info($"Unable to remove credentials because the specified key did not exist: {key}");
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void SaveCredentials(string key, string token)
        {
            try
            {
                _lock.EnterWriteLock();
                s_logger.Debug($"Saving the credentials for key: {key}");
                byte[] byteArray = Encoding.Unicode.GetBytes(token);
                Credential credential = new Credential();
                credential.AttributeCount = 0;
                credential.Attributes = IntPtr.Zero;
                credential.Comment = IntPtr.Zero;
                credential.TargetAlias = IntPtr.Zero;
                credential.Type = 1; // Generic
                credential.Persist = 2; // Local Machine
                credential.CredentialBlobSize = (uint)(byteArray == null ? 0 : byteArray.Length);
                credential.TargetName = key;
                credential.CredentialBlob = token;
                credential.UserName = Environment.UserName;

                CredWrite(ref credential, 0);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct Credential
        {
            public uint Flags;
            public uint Type;
            [MarshalAs(UnmanagedType.LPWStr)]
            public string TargetName;
            public IntPtr Comment;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten;
            public uint CredentialBlobSize;
            [MarshalAs(UnmanagedType.LPWStr)]
            public string CredentialBlob;
            public uint Persist;
            public uint AttributeCount;
            public IntPtr Attributes;
            public IntPtr TargetAlias;
            [MarshalAs(UnmanagedType.LPWStr)]
            public string UserName;
        }

        sealed class CriticalCredentialHandle : CriticalHandleZeroOrMinusOneIsInvalid
        {
            public CriticalCredentialHandle(IntPtr handle)
            {
                SetHandle(handle);
            }

            public Credential GetCredential()
            {
                var credential = (Credential)Marshal.PtrToStructure(handle, typeof(Credential));
                return credential;
            }

            protected override bool ReleaseHandle()
            {
                if (IsInvalid)
                {
                    return false;
                }

                CredFree(handle);
                SetHandleAsInvalid();
                return true;
            }
        }

        [DllImport("Advapi32.dll", EntryPoint = "CredDeleteW", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern bool CredDelete(string target, uint type, int reservedFlag);

        [DllImport("Advapi32.dll", EntryPoint = "CredReadW", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern bool CredRead(string target, uint type, int reservedFlag, out IntPtr credentialPtr);

        [DllImport("Advapi32.dll", EntryPoint = "CredWriteW", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern bool CredWrite([In] ref Credential userCredential, [In] uint flags);

        [DllImport("Advapi32.dll", EntryPoint = "CredFree", SetLastError = true)]
        static extern bool CredFree([In] IntPtr cred);
    }
}
