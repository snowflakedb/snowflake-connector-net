REM Run whitesource for components which need versioning

SET SCAN_DIRECTORIES="%cd%"

IF "%PRODUCT_NAME%"=="" SET PRODUCT_NAME="Snowflake_Drivers_Connectors"

SET PROJECT_NAME="Dot-Net-Connector"

SET DATE="%date%"

curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

SET WSS_CONFIG="wss-net.config"

java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
   -c %WSS_CONFIG%^
   -project %PROJECT_NAME%^
   -product %PRODUCT_NAME%^
   -projectVersion %DATE%^
   -d %SCAN_DIRECTORIES%^
   -wss.url https://saas.whitesourcesoftware.com/agent^
   -offline true

java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
    -c %WSS_CONFIG%^
    -project %PROJECT_NAME%^
    -product %PRODUCT_NAME%^
    -projectVersion baseline^
    -requestFiles whitesource\update-request.txt^
    -wss.url https://saas.whitesourcesoftware.com/agent

IF NOT %ERRORLEVEL% 0 (
	ECHO "checkPolicies=false" >> %WSS_CONFIG% && java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
	    -c %WSS_CONFIG%^
	    -project %PROJECT_NAME%^
	    -product %PRODUCT_NAME%^
	    -projectVersion %DATE%^
	    -requestFiles whitesource\update-request.txt^
	    -wss.url https://saas.whitesourcesoftware.com/agent
)

