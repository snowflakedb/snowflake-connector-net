/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
    class MockSnowflakeDbConnection : SnowflakeDbConnection
    {
        private SFLogger logger = SFLoggerFactory.GetLogger<MockSnowflakeDbConnection>();

        public override void Open()
        {
            logger.Debug("Open Connection.");
            SetMockSession();
            try
            {
                SfSession.Open();
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                _connectionState = System.Data.ConnectionState.Closed;
                logger.Error("Unable to connect", e);
                throw;
            }
            OnSessionEstablished();
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            registerConnectionCancellationCallback(cancellationToken);

            SetMockSession();

            return SfSession.OpenAsync(cancellationToken).ContinueWith(
                previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                    // Exception from SfSession.OpenAsync
                    Exception sfSessionEx = previousTask.Exception;
                        _connectionState = ConnectionState.Closed;
                        logger.Error("Unable to connect", sfSessionEx.InnerException);
                        throw //sfSessionEx.InnerException;
                        new SnowflakeDbException(sfSessionEx.InnerException, SFError.INTERNAL_ERROR, "Unable to connect");
                    }
                    if (previousTask.IsCanceled)
                    {
                        _connectionState = ConnectionState.Closed;
                        logger.Debug("Connection canceled");
                    }
                    else
                    {
                        OnSessionEstablished();
                    }
                },
                cancellationToken);

        }

        private void SetMockSession()
        {
            var restRequester = new MockRetryUntilRestTimeoutRestRequester();
            SfSession = new SFSession(ConnectionString, Password, restRequester);

            _connectionTimeout = (int)SfSession.connectionTimeout.TotalSeconds;

            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            _connectionState = ConnectionState.Open;
        }
    }
}
