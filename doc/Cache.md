## Cache

The Snowflake .NET driver provides the ability to cache tokens using different implementations.

### Types of Cache
- **In-memory implementation:** The most secure option. Stores credentials that persist within the application's runtime
- **Windows Credential Store:** The second most secure option. Leverages built-in Windows capabilities to cache tokens in the Windows Credential Store
- **File-based implementation:** The least secure option. Stores credentials in a JSON file on the system

### Avaiable Cache Type by Operating System
**Windows**
- In-memory implementation
- Windows Credential Store

**Linux**
- In-memory implementation
- File-based implementation

**Mac**
- In-memory implementation
- File-based implementation
