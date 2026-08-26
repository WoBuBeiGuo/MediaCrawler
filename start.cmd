@echo off
setlocal

set "PROJECT_DIR=%~dp0"

if exist "%PROJECT_DIR%start.local.cmd" (
    call "%PROJECT_DIR%start.local.cmd"
    if errorlevel 1 (
        echo [ERROR] Failed to load local configuration:
        echo         %PROJECT_DIR%start.local.cmd
        exit /b 1
    )
)

set "PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe"
set "MODE=%~1"

if "%MODE%"=="" set "MODE=all"

if not exist "%PYTHON%" (
    echo [ERROR] Python virtual environment was not found:
    echo         %PYTHON%
    echo.
    echo Please initialize the MediaCrawler .venv first.
    pause
    exit /b 1
)

if /I "%MODE%"=="all" goto start_all
if /I "%MODE%"=="service" goto start_all
if /I "%MODE%"=="worker" goto start_worker
if /I "%MODE%"=="webui" goto start_webui
if /I "%MODE%"=="help" goto show_help
if /I "%MODE%"=="--help" goto show_help
if /I "%MODE%"=="-h" goto show_help

echo [ERROR] Unknown mode: %MODE%
echo.
goto show_help_error

:start_all
cd /d "%PROJECT_DIR%"
echo Starting unified MediaCrawler service on http://127.0.0.1:18082 ...
echo   Embedded Worker: Java task collection and database ingestion
echo   WebUI API      : Native MediaCrawler console
echo   Chrome         : One persistent session for the Worker lifetime
echo.
"%PYTHON%" -m uvicorn api.main:app --host 127.0.0.1 --port 18082
exit /b %ERRORLEVEL%

:start_worker
cd /d "%PROJECT_DIR%"
echo Starting RuoYi Media Worker...
"%PYTHON%" -m integration.ruoyi_media.worker
exit /b %ERRORLEVEL%

:start_webui
cd /d "%PROJECT_DIR%"
echo Starting MediaCrawler WebUI without the embedded Worker on http://127.0.0.1:18082 ...
set "RUOYI_MEDIA_EMBEDDED_WORKER=false"
"%PYTHON%" -m uvicorn api.main:app --host 127.0.0.1 --port 18082
exit /b %ERRORLEVEL%

:show_help
echo Usage:
echo   start.cmd             Start the unified Worker and WebUI service
echo   start.cmd service     Start the unified Worker and WebUI service
echo   start.cmd worker      Start only the RuoYi Media Worker
echo   start.cmd webui       Start only the MediaCrawler WebUI, without Worker
echo   start.cmd help        Show this help
exit /b 0

:show_help_error
echo Usage:
echo   start.cmd             Start the unified Worker and WebUI service
echo   start.cmd service     Start the unified Worker and WebUI service
echo   start.cmd worker      Start only the RuoYi Media Worker
echo   start.cmd webui       Start only the MediaCrawler WebUI, without Worker
echo   start.cmd help        Show this help
exit /b 2
