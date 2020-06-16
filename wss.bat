REM Run whitesource for components which need versioning
@echo off
setlocal

if not defined WHITESOURCE_API_KEY (
    echo == No WHITESOURCE_API_KEY is set. Skipping WhiteSource scan
    exit /b 0
)
SET SCAN_DIRECTORIES=%cd%

SET PRODUCT_NAME=snowflake-connector-net

REM If your PROD_BRANCH is not master, you can define it here based on the need
SET PROD_BRANCH=master

echo branch: %APPVEYOR_REPO_BRANCH%
echo commit: %APPVEYOR_REPO_COMMIT%
echo pr-number: %APPVEYOR_PULL_REQUEST_NUMBER%

REM Exit Whitesource scanning if no PR is defined in the Job
if not defined APPVEYOR_PULL_REQUEST_NUMBER (
    if not "%APPVEYOR_REPO_BRANCH%" == "%PROD_BRANCH%" (
        ECHO == APPVEYOR_PULL_REQUEST_NUMBER is NOT defined. Skipping wss.sh
        EXIT /b 0
    )
    SET PROJECT_NAME=%PROD_BRANCH%
) else (
    SET PROJECT_NAME=PR-%APPVEYOR_PULL_REQUEST_NUMBER%
)

SET PROJECT_VERSION=%APPVEYOR_REPO_COMMIT%

curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar
IF %ERRORLEVEL% NEQ 0 (
    ECHO == failed to download whitesource unified agent
    REM exit /b can capture the error when failing to download whitesource unified agent but will NOT fail the build
    EXIT /b 1
    REM if you want to fail the build when failing to download whitesource unified agent, please use exit /b 1 instead
)

SET WSS_CONFIG=wss-net.config
COPY %WSS_CONFIG%.templ %WSS_CONFIG%

IF %PROJECT_NAME%==%PROD_BRANCH% (
  java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
     -c %WSS_CONFIG%^
     -project %PROJECT_NAME%^
     -product %PRODUCT_NAME%^
     -projectVersion %PROJECT_VERSION%^
     -d %SCAN_DIRECTORIES%^
     -wss.url https://saas.whitesourcesoftware.com/agent^
     -offline true
     IF %ERRORLEVEL% NEQ -2 (
        IF %ERRORLEVEL% NEQ 0 (
          ECHO == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% in offline mode 
          REM exit /b can capture the error when failing to run whitesource in offline mode but will NOT fail the build
          EXIT /b 1
          REM if you want to fail the build when failing to run whitesource in offline mode, please use exit /b 1 instead
        )
     )
  java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
      -c %WSS_CONFIG%^
      -product %PRODUCT_NAME%^
      -project %PROJECT_NAME%^
      -projectVersion baseline^
      -requestFiles whitesource\update-request.txt^
      -wss.url https://saas.whitesourcesoftware.com/agent
      IF %ERRORLEVEL% NEQ -2 (
        IF %ERRORLEVEL% NEQ 0 (
          ECHO == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% with baseline 
          REM exit /b can capture the error when failing to run whitesource with projectName baseline but will NOT fail the build
          EXIT /b 1
          REM if you want to fail the build when failing to run whitesource with projectName baseline, please use exit /b 1 instead
        )
      )
  java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
      -c %WSS_CONFIG%^
      -product %PRODUCT_NAME%^
      -project %PROJECT_NAME%^
      -projectVersion %PROJECT_VERSION%^
      -requestFiles whitesource\update-request.txt^
      -wss.url https://saas.whitesourcesoftware.com/agent
      IF %ERRORLEVEL% NEQ -2 (
        IF %ERRORLEVEL% NEQ 0 (
          ECHO == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% in %PROJECT_VERSION%
          REM exit /b can capture the error when failing to run whitesource with projectName GIT_COMMIT but will NOT fail the build
          EXIT /b 1
          REM if you want to fail the build when failing to run whitesource with projectName GIT_COMMIT, please use exit /b 1 instead
        )
      )
) ELSE (
  java -jar wss-unified-agent.jar -apiKey %WHITESOURCE_API_KEY%^
      -c %WSS_CONFIG%^
      -project %PROJECT_NAME%^
      -product %PRODUCT_NAME%^
      -projectVersion %PROJECT_VERSION%^
      -d %SCAN_DIRECTORIES%
      IF %ERRORLEVEL% NEQ -2 (
        IF %ERRORLEVEL% NEQ 0 (
          ECHO == failed to run WSS for %PRODUCT_NAME%_%PROJECT_NAME% in %PROJECT_VERSION%
          REM exit /b can capture the error when failing to run whitesource with projectName feature branch but will NOT fail the build
          EXIT /b 1
          REM if you want to fail the build when failing to run whitesource with projectName feature branchT, please use exit /b 1 instead
        )
      )
)
