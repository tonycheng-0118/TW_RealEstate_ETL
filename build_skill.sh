#!/bin/bash
# build_skill.sh — Package the repo into a distributable Claude Code skill folder.
#
# Output: dist/tw-realestate-etl/
# Install: cp -r dist/tw-realestate-etl ~/.claude/skills/
set -euo pipefail

DIST=dist/tw-realestate-etl

rm -rf "$DIST"
mkdir -p "$DIST/scripts" "$DIST/sql" "$DIST/launchd"

# Unified SKILL.md (single entry point for all 7 operations)
cp claude-skill/tw-realestate-etl/SKILL.md "$DIST/"

# Python scripts (all ETL pipeline modules)
cp scripts/__init__.py "$DIST/scripts/"
cp scripts/download.py "$DIST/scripts/"
cp scripts/transform.py "$DIST/scripts/"
cp scripts/load.py "$DIST/scripts/"
cp scripts/backup.py "$DIST/scripts/"
cp scripts/run_etl.py "$DIST/scripts/"

# Config module — copied from project root INTO scripts/ for skill layout.
# The dual-mode detection in config.py handles both locations.
cp config.py "$DIST/scripts/"

# Python dependencies reference
cp requirements.txt "$DIST/scripts/"

# PostgreSQL DDL (idempotent schema creation)
cp sql/schema.sql "$DIST/sql/"

# macOS LaunchAgent template
cp launchd/com.tw-realestate.etl.plist "$DIST/launchd/"

echo ""
echo "=== Skill packaged successfully ==="
echo "Output: $DIST/"
echo ""
echo "Install:"
echo "  cp -r $DIST ~/.claude/skills/"
echo ""
echo "Or create a release zip:"
echo "  cd dist && zip -r tw-realestate-etl.zip tw-realestate-etl/"
