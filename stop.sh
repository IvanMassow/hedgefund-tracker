#!/bin/bash
# Stop Hedge Fund Edge Tracker daemon
cd "$(dirname "$0")"

if [ -f .hedgefund.pid ]; then
    PID=$(cat .hedgefund.pid)
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping Hedge Fund Edge Tracker (PID $PID)..."
        kill "$PID"
        rm .hedgefund.pid
        echo "Stopped."
    else
        echo "PID $PID not running. Cleaning up."
        rm .hedgefund.pid
    fi
else
    echo "No PID file found. May not be running."
    echo "Check: ps aux | grep runner.py"
fi
