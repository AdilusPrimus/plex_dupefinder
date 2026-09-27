#!/bin/sh

# Run Plex DupeFinder with Datadog APM enabled (IAST disabled due to probe conflicts)
DD_ENV=production DD_SERVICE=plex_dupefinder DD_VERSION=1.0.6 DD_TRACE_DEBUG=true DD_TRACE_LOG_LEVEL=ERROR DD_IAST_ENABLED=true ddtrace-run python3 plex_dupefinder.py
