REM Run whitesource for components which need versioning
setlocal

SET SCAN_DIRECTORIES="%cd%"

SET PRODUCT_NAME=DotNETDriver
SET PROJECT_NAME=DotNETDriver

REM Format MM-DD-YYYY
SET CURRENT_DATE=%date:~4,2%-%date:~7,2%-%date:~10,4%

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

IF %ERRORLEVEL% NEQ 0 (
	ECHO "checkPolicies=false" >> %WSS_CONFIG% && java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
	    -c %WSS_CONFIG%^
	    -project %PROJECT_NAME%^
	    -product %PRODUCT_NAME%^
	    -projectVersion %CURRENT_DATE%^
	    -requestFiles whitesource\update-request.txt^
	    -wss.url https://saas.whitesourcesoftware.com/agent
)

