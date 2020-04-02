REM Run whitesource for components which need versioning

SET SCAN_DIRECTORIES="%cd%"

IF "%PRODUCT_NAME%"=="" SET PRODUCT_NAME="Snowflake_Drivers_Connectors"

SET PROJECT_NAME="Dot-Net-Connector"

SET DATE=%date:~4,2%-%date:~7,2%-%date:~10,4%

curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

SET WSS_CONFIG="wss-net.config"

java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
   -c %WSS_CONFIG%^
   -project %PROJECT_NAME%^
   -product %PRODUCT_NAME%^
   -projectVersion %DATE%^
   -d %SCAN_DIRECTORIES%

