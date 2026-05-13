#!/bin/bash
# Run Silver lifecycle materialization as a standalone batch job.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "${SCRIPT_DIR}/run_silver.sh" lifecycle batch
