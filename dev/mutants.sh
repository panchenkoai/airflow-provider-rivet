#!/bin/sh
# Mutation testing of airflow_provider_rivet/ in a Linux container (README, "Mutation testing"); `make mutants`.
# Linux, never the host's macOS Python: mutmut forks a worker per mutant, and macOS aborts a forked child of a
# multithreaded process that touches Objective-C (psutil under Airflow's import), so a mutant's verdict there is the
# crash, not its tests. Linux also runs the PR_SET_PDEATHSIG test of tests/test_retry.py.
# The suite runs as the calling user, not root: root writes into a 0o500 directory and fails a preflight test.
#   dev/mutants.sh                        -> mutmut run (resumable: results persist in mutants/)
#   dev/mutants.sh mutmut results         -> any other command in the same environment
# RIVET_SRC: a rivet checkout at RIVET_CONTRACT_REF, as for pytest (default ../rivet).
set -eu
cd "$(dirname "$0")/.."
RIVET_SRC="$(cd "${RIVET_SRC:-../rivet}" && pwd)"
MUTMUT="${MUTMUT:-mutmut==3.8.0}"
IMAGE="${MUTANTS_IMAGE:-ghcr.io/astral-sh/uv:python3.13-bookworm}"
[ "$#" -gt 0 ] || set -- mutmut run --max-children "${MUTANTS_MAX_CHILDREN:-3}"
# Capped (MUTANTS_DOCKER_ARGS overrides): the run shares the Docker VM with whatever else the host runs, and a
# mutant may grow memory without bound (dev/mutmut_isolation.py caps each worker as well).
# shellcheck disable=SC2086
exec docker run --rm --init ${MUTANTS_DOCKER_ARGS:---memory 4g --cpus 3} \
  -v "$PWD:/work" -v "$RIVET_SRC:/rivet:ro" \
  -v apr-mutants-venv:/venv -v apr-mutants-uvcache:/uvcache \
  -e UV_PROJECT_ENVIRONMENT=/venv -e UV_CACHE_DIR=/uvcache -e UV_LINK_MODE=copy -e RIVET_SRC=/rivet \
  -e PYTHONPATH=/work/dev -e RUN_UID="$(id -u)" -e RUN_GID="$(id -g)" \
  -w /work "$IMAGE" sh -c '
    command -v git >/dev/null || { apt-get -qq update && apt-get -qq install -y git; } >/dev/null
    chown -R "$RUN_UID:$RUN_GID" /venv /uvcache
    exec setpriv --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups env HOME=/tmp USER=mutants LOGNAME=mutants \
      sh -c "uv sync --locked -q && exec uv run --with \"\$0\" \"\$@\"" "$0" "$@"' "$MUTMUT" "$@"
