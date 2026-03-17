#!/bin/bash
# Wöchentlicher LinkedIn LegalTech Tracker
# Ausgeführt via cron: Sonntag 20:00 (Daten für die vergangene Woche)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/venv/bin/python"
LOG="$SCRIPT_DIR/output/weekly/track.log"

mkdir -p "$SCRIPT_DIR/output/weekly"

# Ins Projektverzeichnis wechseln, damit relative Pfade in track.py
# (OUTPUT_DIR = "output/weekly") korrekt aufgelöst werden.
cd "$SCRIPT_DIR"

echo "=== $(date) ===" >> "$LOG"
"$VENV" "$SCRIPT_DIR/track.py" >> "$LOG" 2>&1
echo "" >> "$LOG"

echo "=== Eigene Posts: $(date) ===" >> "$LOG"
"$VENV" "$SCRIPT_DIR/track_own.py" >> "$LOG" 2>&1
echo "" >> "$LOG"
