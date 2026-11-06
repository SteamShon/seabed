#!/usr/bin/env bash
# Fails a PR that changes code or CI without touching docs/STATUS.md, unless the PR body
# says why with a line "STATUS: 해당 없음 — <이유>" (an ASCII "-" also works). AGENTS.md
# "진행 상황 기록" has the rule; this check only makes sure the decision is not forgotten.
#
# Usage: status-check.sh <changed-files-file>   (PR body in the PR_BODY environment variable)
set -euo pipefail

changed="$1"
watched='^(crates/|\.github/|Cargo\.(toml|lock)$)'

if ! grep -Eq "$watched" "$changed"; then
  echo "ok: no code or CI change"
  exit 0
fi
if grep -qx 'docs/STATUS.md' "$changed"; then
  echo "ok: docs/STATUS.md is updated"
  exit 0
fi
if printf '%s\n' "${PR_BODY:-}" | tr -d '\r' | grep -Eq '^STATUS: 해당 없음 *(—|-) *[^ ].*'; then
  echo "ok: PR body says STATUS.md needs no change:"
  printf '%s\n' "${PR_BODY:-}" | tr -d '\r' | grep -E '^STATUS: 해당 없음'
  exit 0
fi
echo "::error::This PR changes code or CI but not docs/STATUS.md."
echo "If a line of docs/STATUS.md is now wrong, fix it in this PR (AGENTS.md, 진행 상황 기록)."
echo "If none is, add this line to the PR body: STATUS: 해당 없음 — <이유>"
exit 1
