#!/bin/sh

# Run Plex DupeFinder with Datadog IAST enabled
DD_TRACE_DEBUG=false DD_TRACE_LOG_LEVEL=ERROR DD_IAST_ENABLED=true ddtrace-run python3 plex_dupefinder.py