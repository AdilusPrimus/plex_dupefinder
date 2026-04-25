#!/bin/sh

# Run Plex DupeFinder with Datadog IAST enabled
DD_IAST_ENABLED=true ddtrace-run python3 plex_dupefinder.py