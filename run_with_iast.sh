#!/bin/sh

# Run Plex DupeFinder with Datadog APM enabled (IAST disabled due to probe conflicts)
DD_TRACE_DEBUG=false DD_TRACE_LOG_LEVEL=ERROR DD_IAST_ENABLED=false ddtrace-run python3 plex_dupefinder.py