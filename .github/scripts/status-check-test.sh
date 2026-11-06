#!/usr/bin/env bash
# Cases for status-check.sh. Run: .github/scripts/status-check-test.sh
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
bad=0

# case <name> <changed files, space separated> <PR body> <pass|fail>
case_() {
  printf '%s\n' $2 > "$tmp/changed.txt"
  if PR_BODY="$3" "$here/status-check.sh" "$tmp/changed.txt" > /dev/null 2>&1; then got=pass; else got=fail; fi
  if [ "$got" = "$4" ]; then echo "ok   $1"; else echo "FAIL $1: got $got, want $4"; bad=1; fi
}

case_ "docs only"           "docs/PLAN.md"                             ""                                      pass
case_ "code, no STATUS"     "crates/engine/src/wal.rs"                 "본문"                                  fail
case_ "code with STATUS"    "crates/engine/src/wal.rs docs/STATUS.md"  ""                                      pass
case_ "exempt, em dash"     "crates/engine/src/wal.rs"                 $'요약\nSTATUS: 해당 없음 — 테스트만'    pass
case_ "exempt, hyphen, CR"  "crates/engine/src/wal.rs"                 $'STATUS: 해당 없음 - 내부 정리\r'       pass
case_ "exempt, no reason"   "crates/engine/src/wal.rs"                 "STATUS: 해당 없음 —"                   fail
case_ "exempt mid-line"     "crates/engine/src/wal.rs"                 "참고 STATUS: 해당 없음 — x"            fail
case_ "CI change"           ".github/workflows/ci.yml"                 ""                                      fail
case_ "lockfile"            "Cargo.lock"                               ""                                      fail
case_ "STATUS look-alike"   "crates/x.rs docs/STATUS.md.bak"           ""                                      fail
case_ "empty diff"          ""                                         ""                                      pass
exit $bad
