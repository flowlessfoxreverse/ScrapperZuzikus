#!/bin/sh
set -eu

chmod og+rx /db || true
exec "$@"
