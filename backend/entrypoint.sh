#!/bin/sh
# Fix ownership of mounted volumes (they may be root-owned)
chown -R appuser:appuser /app/sessions /app/media 2>/dev/null || true
exec gosu appuser "$@"
