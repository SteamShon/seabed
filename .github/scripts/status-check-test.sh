#!/usr/bin/env bash
# Cases for status-changed-files.sh and status-check.sh. Run: .github/scripts/status-check-test.sh
set -uo pipefail
set -f # the file lists below are word-split on purpose; never glob them
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
bad=0

# check <name> <list file> <PR body> <pass|fail>
check() {
  if PR_BODY="$3" "$here/status-check.sh" "$2" > /dev/null 2>&1; then got=pass; else got=fail; fi
  if [ "$got" = "$4" ]; then echo "ok   $1"; else echo "FAIL $1: got $got, want $4"; bad=1; fi
}

# case_ <name> <changed files, space separated; empty for none> <PR body> <pass|fail>
case_() {
  : > "$tmp/changed.txt"
  if [ -n "$2" ]; then printf '%s\n' $2 > "$tmp/changed.txt"; fi
  check "$1" "$tmp/changed.txt" "$3" "$4"
}

case_ "docs only"                  "docs/PLAN.md"                             ""                                      pass
case_ "code, no STATUS"            "crates/engine/src/wal.rs"                 "본문"                                  fail
case_ "code with STATUS"           "crates/engine/src/wal.rs docs/STATUS.md"  ""                                      pass
case_ "exempt, em dash"            "crates/engine/src/wal.rs"                 $'요약\nSTATUS: 해당 없음 — 테스트만'    pass
case_ "exempt, hyphen, CR"         "crates/engine/src/wal.rs"                 $'STATUS: 해당 없음 - 내부 정리\r'       pass
case_ "exempt, no reason"          "crates/engine/src/wal.rs"                 "STATUS: 해당 없음 —"                   fail
case_ "exempt, no reason, CR"      "crates/engine/src/wal.rs"                 $'STATUS: 해당 없음 —\r'                 fail
case_ "exempt mid-line"            "crates/engine/src/wal.rs"                 "참고 STATUS: 해당 없음 — x"            fail
case_ "CI change"                  ".github/workflows/ci.yml"                 ""                                      fail
case_ "lockfile"                   "Cargo.lock"                               ""                                      fail
case_ "Cargo look-alike"           "Cargo.toml.orig"                          ""                                      pass
case_ "nested crates dir"          "docs/crates/x.md"                         ""                                      pass
case_ "STATUS look-alike"          "crates/x.rs docs/STATUS.md.bak"           ""                                      fail
case_ "empty diff"                 ""                                         ""                                      pass
check "missing list"               "$tmp/does-not-exist.txt"                  ""                                      fail

# The list as status-changed-files.sh makes it, from a real repository.
repo="$tmp/repo"
git init -q "$repo"
(
  cd "$repo"
  git config user.email t@example.com
  git config user.name t
  mkdir -p crates/a docs
  echo one > crates/a/lib.rs
  echo doc > docs/PLAN.md
  git add -A && git commit -qm base && git branch -q base
) || bad=1
listed() { (cd "$repo" && "$here/status-changed-files.sh" base > "$tmp/list.txt"); }

(cd "$repo" && git checkout -q -b non-ascii base && echo x > "crates/a/한글.rs" && git add -A && git commit -qm na)
listed && check "non-ASCII path is listed plainly" "$tmp/list.txt" "" fail
grep -qx 'crates/a/한글.rs' "$tmp/list.txt" || { echo "FAIL non-ASCII path text: $(cat "$tmp/list.txt")"; bad=1; }

(cd "$repo" && git checkout -q -b moved base && mkdir -p other && git mv crates/a/lib.rs other/lib.rs && git commit -qm mv)
listed && check "move out of crates/ still counts" "$tmp/list.txt" "" fail

exit $bad
