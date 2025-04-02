## Cache

The Snowflake .NET driver provides the ability to cache tokens using different implementations.

### Enabling the Cache

The SSO token cache is enabled by setting the parameter **client_store_temporary_credential** to true. The default value is based on OS:
- **Windows**: true
- **Linux**: false
- **Mac**: false

### Types of Cache
- **In-memory implementation:** The most secure option. Stores credentials that persist within the application's runtime
- **Windows Credential Manager:** The second most secure option. Leverages built-in Windows capabilities to cache tokens in the Windows Credential Manager
- **File-based implementation:** The least secure option. Stores credentials in a JSON file on the system
- **Custom implementation:** Users can choose to implement their own version of the cache

### Avaiable Cache Type by Operating System
**Windows**
- Windows Credential Manager (default)
- In-memory implementation

**Linux**
- File-based implementation (default)
- In-memory implementation

**Mac**
- File-based implementation (default)
- In-memory implementation

### Switching Cache Type

To switch the cache type, simply call the following method based on your preference:
- **In-memory implementation:**
```cs
SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();
```
- **Windows Credential Manager:**
```cs
SnowflakeCredentialManagerFactory.UseWindowsCredentialManager();
```
- **File-based implementation:**
```cs
SnowflakeCredentialManagerFactory.UseFileCredentialManager();
```
- **Custom implementation:**
```cs
SnowflakeCredentialManagerFactory.SetCredentialManager(CustomCredentialManagerImplementation);
```
