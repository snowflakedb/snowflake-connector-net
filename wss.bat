REM Run whitesource for components which need versioning
@echo off
setlocal

if not defined WHITESOURCE_API_KEY (
    echo == No WHITESOURCE_API_KEY is set. Skipping WhiteSource scan
    exit /b 0
)
SET SCAN_DIRECTORIES="%cd%"

SET PRODUCT_NAME=snowflake-connector-net
SET PROJECT_NAME=snowflake-connector-net

REM Format MM-DD-YYYY
SET CURRENT_DATE=%date:~4,2%-%date:~7,2%-%date:~10,4%

curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

SET WSS_CONFIG="wss-net.config"
COPY %WSS_CONFIG%.templ %WSS_CONFIG%

java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
   -c %WSS_CONFIG%^
   -project %PROJECT_NAME%^
   -product %PRODUCT_NAME%^
   -d %SCAN_DIRECTORIES%^
   -wss.url https://saas.whitesourcesoftware.com/agent^
   -offline true
IF %ERRORLEVEL% NEQ 0 (
    echo == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% in offline mode
    exit /b 1
)

java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
    -c %WSS_CONFIG%^
    -product %PRODUCT_NAME%^
    -project %PROJECT_NAME%^
    -projectVersion baseline^
    -requestFiles whitesource\update-request.txt^
    -wss.url https://saas.whitesourcesoftware.com/agent

IF %ERRORLEVEL% NEQ 0 (
    echo == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% with baseline
    exit /b 1
)
echo checkPolicies=false>>%WSS_CONFIG%
java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
    -c %WSS_CONFIG%^
    -product %PRODUCT_NAME%^
    -project %PROJECT_NAME%^
    -projectVersion %CURRENT_DATE%^
    -requestFiles whitesource\update-request.txt^
    -wss.url https://saas.whitesourcesoftware.com/agent

