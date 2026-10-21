# Capabilities

What the package does today when the installed rivet lacks a feature of the scheduler contract.

## Works today / needs a newer rivet

ADR-0039 describes several things no released binary emits yet. The package detects what the
installed binary can do and takes a recorded fallback for the rest: every task's XCom carries
`degraded: [<key>, ...]`; the task log names the missing flags in one `note` line (`this binary
lacks: ...`) and the other keys in words, and the `rivet.result` record at DEBUG carries the list.
Nothing is guessed without a key.

**How it detects.** A flag is detected by probing `rivet <subcommand> --help` once per process
(not by a version table: the features below have no release number yet, and a development or
backported build has a flag whatever its version string says). An output member (`class`,
`failures[]`, `stop_reason`) is detected by its presence in the output itself. The version is
used for one thing only: the minimum-version refusal.

"Needs" names the ADR-0039 section; none of it is in a released rivet at this package's date,
so the "rivet >=" column is the first release that carries that section.

| Key | Missing in rivet 0.32.0 | What the package does today | Needs (ADR-0039) |
|---|---|---|---|
| `no_notify` | global `--no-notify` | does not pass it; warns when the config has a `notifications:` block | D9, rivet >= unreleased |
| `lock_wait` | global `--lock-wait` and the per-export run lease | does not pass it; the builders set `max_active_runs=1` and `max_active_tis_per_dag=1` | D7, rivet >= unreleased |
| `error_object` | `class`, `retryable`, `kind`, `action` on the `--json-errors` line | derives `class` and `retryable` from the printed line's integer `exit_class`; `kind` and `action` are null | D1/D2, rivet >= unreleased |
| `per_unit_error` | `failures[]` and `per_export[].error` | when several units of one process failed, each carries the process-level object | D2/D3, rivet >= unreleased |
| `stop_reason` | CDC `stop_reason` and `tables[]` | `stop_reason` is null: a `max_events` stop cannot be told from `caught_up` | D4, rivet >= unreleased |
| `load_filter` | `--export` on `rivet load` / `rivet compact` | writes a single-export copy of the config and loads that: the other `- name:` items are cut out of the original text, so what stays (quoted `${VAR}`, comments, anchors) is byte-exact; a stream keeps the batch exports its `cdc.backfill` reads; a text that cannot be cut safely (an anchor a kept item reads, an item sharing a line) is refused before rivet starts (`RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE`) | D3, rivet >= unreleased |
| `load_table_filter` | `--table` on `rivet load` / `rivet compact` | the per-table task handles every table of its export; tasks of one export are serialised by a file lock in the state directory | D3, rivet >= unreleased |
| `load_result` | `--summary-output` on `rivet load` / `rivet compact` | status is `loaded` / `compacted` on exit 0 and `failed` otherwise; `skipped`, `table` and row counts are unknown (null) | D3, rivet >= unreleased |

Six more keys record something the worker or one output lacks, not the binary's version:

| Key | What is missing | What the package does |
|---|---|---|
| `stderr_temp` | a state directory or `stderr_dir` on the worker | writes rivet's stdout and stderr to a private per-user directory under the temp directory (see Secrets); a pod loses them with its disk |
| `crashed_ledger` | a readable and writable crashed-budget ledger | without a state directory: a crash is retried only within the first `crashed_retries` tries of the task instance; with a ledger that cannot be read or written: no crash retry at all |
| `result_malformed` | an error object or a result entry with the contract's keys and types; also a stderr line that starts with `{` and is not JSON, or a summary file of an exit-0 process that is not a JSON object | a malformed error object is ignored and the task is classified by the exit-code table; a value of the wrong type is null; a status outside the contract's words counts as `failed` |
| `load_digest` | the digest of a `rivet load` / `rivet compact` entry (`target`, `rows_inserted` ... `bytes_billed`, ADR-0039 D3, rivet >= 0.32.2) | the unit's warehouse object is the config's `load:` block's, marked `(from config)` in the task log, and is null in XCom; the log says nothing of what the warehouse did (counts, jobs, bytes) |
| `result_missing` | the `--summary-output` file of a process that exited 0 | rivet could not write it (it only warns); the units are built as if the binary had no summary flag: one unit per requested export or table, status from the exit |
| `exit_zero_failed_unit` | a non-zero exit when the summary reports a failed unit | the task fails, classified by the worst failed unit (`data_integrity` > `internal` > `refusal` > `schema_drift` > `retryable` > `generic`, ADR-0039 D2) |

Not detectable, so not recorded per task:

- A child process killed by a signal is not reported as `crashed` by today's binary: the parent
  exits 1 (or 2, by a text match) and the task follows that exit. A kill of the rivet process
  the operator started is seen directly and is `crashed` today.
- There is no `RIVET_STATE_FOREIGN` refusal yet. The package's own marker check (below) is the
  only guard against a wrong state directory.
- rivet does not yet warn about a relative `cdc.checkpoint` under PostgreSQL state.

The list lives in one place in the code, `airflow_provider_rivet/capabilities.py`
(`DEGRADATIONS`), and a test fails when a key has no row in this table.
