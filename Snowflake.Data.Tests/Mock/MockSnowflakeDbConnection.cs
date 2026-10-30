using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.Mock;

class MockSnowflakeDbConnection : SnowflakeDbConnection
{
    private readonly Func<SFSession, IAuthenticator> _authenticatorFactory;
    private readonly SFLogger logger = SFLoggerFactory.GetLogger<MockSnowflakeDbConnection>();

    private readonly IMockRestRequester _restRequester;

    public MockSnowflakeDbConnection(IMockRestRequester requester)
    {
        _restRequester = requester;
    }

    public MockSnowflakeDbConnection(Func<SFSession, IAuthenticator> authenticatorFactory) : this()
    {
        _authenticatorFactory = authenticatorFactory;
    }

    public MockSnowflakeDbConnection()
    {
        // Default requester
        _restRequester = new MockRetryUntilRestTimeoutRestRequester();
    }

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
            _connectionState = ConnectionState.Closed;
            logger.Error("Unable to connect", e);
            throw;
        }
        OnSessionEstablished();
    }

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.Register(() => { _connectionState = ConnectionState.Closed; });

        SetMockSession();

        return SfSession.OpenAsync(cancellationToken).ContinueWith(
            previousTask =>
            {
                if (previousTask.IsFaulted)
                {
                    // Exception from SfSession.OpenAsync
                    Exception sfSessionEx = previousTask.Exception;
                    _connectionState = ConnectionState.Closed;
                    logger.Error("Unable to connect", sfSessionEx);
                    throw //sfSessionEx.InnerException;
                        new SnowflakeDbException(sfSessionEx, SFError.INTERNAL_ERROR, args: "Unable to connect");
                }
                if (previousTask.IsCanceled)
                {
                    _connectionState = ConnectionState.Closed;
                    logger.Debug("Connection canceled");
                    throw new SnowflakeDbException(SFError.REQUEST_TIMEOUT, args: ["N/A", "N/A"]);
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
        var sessionContext = new SessionPropertiesContext
        {
            Password = Password,
            Passcode = Passcode
        };
        SfSession = new SFSession(ConnectionString, sessionContext, EasyLoggingStarter.Instance, _restRequester);

        if (_authenticatorFactory != null)
            SfSession.ReplaceAuthenticator(_authenticatorFactory.Invoke(SfSession));

        _connectionTimeout = (int)SfSession.connectionTimeout.TotalSeconds;

        _connectionState = ConnectionState.Connecting;
    }

    private void OnSessionEstablished()
    {
        _connectionState = ConnectionState.Open;
    }

    protected override bool CanReuseSession(TransactionRollbackStatus transactionRollbackStatus)
    {
        return false;
    }
}
