using System.Data;
using System.Data.Common;
using Snowflake.Data.Core;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client;

public class SnowflakeDbTransaction : DbTransaction
{
    private readonly SFLogger _logger = SFLoggerFactory.GetLogger<SnowflakeDbTransaction>();

    private readonly IsolationLevel _isolationLevel;

    private readonly SnowflakeDbConnection _connection;

    private bool _disposed;
    private bool _isCommittedOrRollbacked;

    internal bool IsActive => !_disposed && !_isCommittedOrRollbacked;

    public SnowflakeDbTransaction(IsolationLevel isolationLevel, SnowflakeDbConnection connection)
    {
        _logger.Debug("Begin transaction.");
        if (isolationLevel != IsolationLevel.ReadCommitted)
        {
            throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
        }
        if (connection == null)
        {
            _logger.Error("Transaction cannot be started for an unknown connection");
            throw new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY);
        }
        if (!connection.IsOpen())
        {
            _logger.Error("Transaction cannot be started for a closed connection");
            throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
        }

        this._isolationLevel = isolationLevel;
        this._connection = connection;

        using (IDbCommand command = connection.CreateCommand())
        {
            _isCommittedOrRollbacked = false;
            command.CommandText = "BEGIN";
            command.ExecuteNonQuery();
        }
        connection.ExplicitTransaction = this;
    }

    public override IsolationLevel IsolationLevel => _isolationLevel;

    protected override DbConnection DbConnection => _connection;

    public override void Commit()
    {
        _logger.Debug("Commit transaction.");
        if (!_isCommittedOrRollbacked)
        {
            using (IDbCommand command = _connection.CreateCommand())
            {
                _isCommittedOrRollbacked = true;
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();
            }
        }
    }

    public override void Rollback()
    {
        _logger.Debug("Rollback transaction.");
        if (!_isCommittedOrRollbacked)
        {
            using (IDbCommand command = _connection.CreateCommand())
            {
                _isCommittedOrRollbacked = true;
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        // Rollback the uncommitted transaction when the connection is open
        if (_connection != null && _connection.IsOpen())
        {
            // When there is no uncommitted transaction, Snowflake would just ignore the rollback request;
            if (!_isCommittedOrRollbacked)
            {
                Rollback();
            }
            _isCommittedOrRollbacked = true;
            _connection.ExplicitTransaction = null; // let GC clean it
        }
        _disposed = true;

        base.Dispose(disposing);
    }

    ~SnowflakeDbTransaction()
    {
        Dispose(false);
    }
}
