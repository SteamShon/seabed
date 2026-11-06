#!/usr/bin/env bash
# Prints the files a PR changes relative to <base>, one per line, as plain paths.
# - core.quotePath=false: a non-ASCII path is printed as is, not as "\355..." in quotes,
#   so path patterns still match it.
# - --no-renames: a moved file lists both its old and new path, so moving a file out of a
#   watched directory still counts as a change there.
#
# Usage: status-changed-files.sh <base>   (for a PR: origin/<base branch>)
set -euo pipefail
git -c core.quotePath=false diff --no-renames --name-only "$1...HEAD"
