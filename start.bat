@echo off
chcp 65001 >nul
title YouTube Uploader Server

cd /d "%~dp0"

if not exist ".env" (
    echo [WARN] .env file not found. Server will use default settings.
    echo        Create .env file if you need custom configuration.
    echo.
)

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python not found in PATH.
    echo         Make sure Python 3.11+ is installed and added to PATH.
    pause
    exit /b 1
)

echo Starting YouTube Uploader Server...
echo Press Ctrl+C to stop.
echo.

:: h11 is the safer default on Windows: plain http auto-detection may pick
:: httptools, which surfaces more connection-reset noise on abrupt client
:: disconnects. The server is single-worker, so h11's throughput is fine.
set UVICORN_HTTP=h11
set UVICORN_TIMEOUT_KEEP_ALIVE=300

python run.py

if errorlevel 1 (
    echo.
    echo [ERROR] Server exited with an error.
    pause
)
