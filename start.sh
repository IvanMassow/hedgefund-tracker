#!/bin/bash
# Hedge Fund Edge Tracker - Launcher
# Usage: ./start.sh           (foreground)
#        ./start.sh --background  (daemon mode)
#        ./start.sh --once    (single cycle)

cd "$(dirname "$0")"

# Create virtual environment if needed
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    echo "Setup complete."
else
    source venv/bin/activate
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

if [ "$1" = "--once" ]; then
    echo "Running single cycle..."
    python3 runner.py --once
elif [ "$1" = "--background" ]; then
    echo "Starting Hedge Fund Edge Tracker in background..."
    nohup python3 runner.py > logs/hedgefund_stdout.log 2>&1 &
    PID=$!
    echo $PID > .hedgefund.pid
    echo "Running with PID $PID"
    echo "Logs: tail -f logs/hedgefund.log"
    echo "Stop: ./stop.sh"
else
    echo "Starting Hedge Fund Edge Tracker..."
    python3 runner.py
fi
