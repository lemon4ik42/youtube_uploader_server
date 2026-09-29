#!/usr/bin/env bash

set -u

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

if [[ ! -f ".env" ]]; then
    printf '%s\n' \
        '[WARN] .env file not found. Server will use default settings.' \
        '       Create .env file if you need custom configuration.' \
        ''
fi

if command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
else
    printf '%s\n' \
        '[ERROR] Python not found in PATH.' \
        '        Make sure Python 3.11+ is installed and added to PATH.'
    exit 1
fi

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'; then
    printf '%s\n' \
        '[ERROR] Python 3.11 or newer is required.' \
        '        Install a supported Python version and try again.'
    exit 1
fi

VENV_PYTHON=".venv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
    if [[ ! -f "requirements.txt" ]]; then
        printf '%s\n' '[ERROR] requirements.txt not found.'
        exit 1
    fi

    printf '%s\n' 'Creating virtual environment...'
    if ! "$PYTHON_BIN" -m venv .venv; then
        printf '%s\n' \
            '[ERROR] Could not create the virtual environment.' \
            '        Install the Python venv package and try again.'
        exit 1
    fi

    printf '%s\n' 'Installing dependencies...'
    if ! "$VENV_PYTHON" -m pip install --upgrade pip; then
        printf '%s\n' '[ERROR] Could not upgrade pip.'
        exit 1
    fi
    if ! "$VENV_PYTHON" -m pip install -r requirements.txt; then
        printf '%s\n' '[ERROR] Could not install dependencies.'
        exit 1
    fi
fi

printf '%s\n' \
    'Starting YouTube Uploader Server...' \
    'Press Ctrl+C to stop.' \
    ''

# h11 avoids noisy connection-reset messages from abrupt client disconnects.
export UVICORN_HTTP=h11
export UVICORN_TIMEOUT_KEEP_ALIVE=300

"$VENV_PYTHON" run.py

if [[ $? -ne 0 ]]; then
    printf '%s\n' '' '[ERROR] Server exited with an error.'
    exit 1
fi
