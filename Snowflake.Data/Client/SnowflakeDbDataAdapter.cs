using System;
using System.Data.Common;
using System.Data;

namespace Snowflake.Data.Client;

public class SnowflakeDbDataAdapter : DbDataAdapter, IDbDataAdapter
{
    private static readonly object s_eventRowUpdated = new();
    private static readonly object s_eventRowUpdating = new();

    private SnowflakeDbCommand _selectCommand;

    public SnowflakeDbDataAdapter()
    {
        GC.SuppressFinalize(this);
    }

    public SnowflakeDbDataAdapter(SnowflakeDbCommand selectCommand) : this()
    {
        SelectCommand = selectCommand;
    }

    public SnowflakeDbDataAdapter(string selectCommandText, SnowflakeDbConnection selectConnection) : this()
    {
        SelectCommand = new SnowflakeDbCommand(selectConnection);
        SelectCommand.CommandText = selectCommandText;
    }

    private SnowflakeDbDataAdapter(SnowflakeDbDataAdapter from) : base(from)
    {
        GC.SuppressFinalize(this);
    }

    IDbCommand IDbDataAdapter.DeleteCommand
    {
        get;
        set;
    }

    IDbCommand IDbDataAdapter.InsertCommand
    {
        get;
        set;
    }

    public new SnowflakeDbCommand SelectCommand
    {
        get => _selectCommand;
        set => _selectCommand = value;
    }

    IDbCommand IDbDataAdapter.SelectCommand
    {
        get => _selectCommand;
        set => _selectCommand = (SnowflakeDbCommand)value;
    }

    IDbCommand IDbDataAdapter.UpdateCommand
    {
        get;
        set;
    }
}
