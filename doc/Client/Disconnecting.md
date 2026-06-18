## Close the Connection

To close the connection, call the `Close` method of `SnowflakeDbConnection`.

If you want to avoid blocking threads while the connection is closing, call the `CloseAsync` method instead, passing in a
`CancellationToken`. This method was introduced in the v2.0.4 release.

Note that because this method is not available in the generic `IDbConnection` interface, you must cast the object as
`SnowflakeDbConnection` before calling the method. For example:

```cs
CancellationTokenSource cancellationTokenSource  = new CancellationTokenSource();
// Close the connection
((SnowflakeDbConnection)conn).CloseAsync(cancellationTokenSource.Token);
```

## Evict the Connection

For the open connection, call the `PreventPooling()` to mark the connection to be removed on close instead being still pooled.
The busy sessions counter will be decreased when the connection is closed.

