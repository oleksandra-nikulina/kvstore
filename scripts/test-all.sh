#!/usr/bin/env bash
# Builds, tests, and lints every stage — each one is a fully independent
# Cargo project, so there's no workspace to run this through in one
# `cargo` invocation. CI (.github/workflows/ci.yml) runs the same checks
# per stage in parallel via a matrix; this script is the equivalent for
# a single local run.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

stages=(
  "stage 1 - tcp echo server"
  "stage 2 - thread per connection"
  "stage 3 - resp protocol"
  "stage 4 - core kv store"
  "stage 5 - expiration"
  "stage 6 - data types"
  "stage 7 - async rewrite"
  "stage 8 - persistence"
  "stage 9 - pub sub"
  "stage 10 - eviction"
  "stage 11 - benchmark"
)

failures=()
for stage in "${stages[@]}"; do
  echo "=== $stage ==="
  if (cd "$stage" && cargo test --all-targets -q && cargo clippy --all-targets -q -- -D warnings); then
    echo "--- ok ---"
  else
    echo "--- FAILED ---"
    failures+=("$stage")
  fi
  echo
done

if [ ${#failures[@]} -gt 0 ]; then
  echo "FAILED (${#failures[@]}/${#stages[@]}):"
  printf '  - %s\n' "${failures[@]}"
  exit 1
fi

echo "All ${#stages[@]} stages passed: tests + clippy, zero warnings."
