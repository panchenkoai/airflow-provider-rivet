"""Example: Slack alerts for rivet tasks, and two DAGs that fail on purpose so you can see one arrive.

`slack_failure_callback(conn_id)` in `default_args` posts one message per failed rivet task (never for the
watcher): a title `dag.task — <error type>` and, with a bot token, the full trace in its thread. See docs/slack.md
for the whole setup.

Set on the worker before the scheduler parses this file:
  RIVET_EXAMPLE_STATE_DIR  an existing directory on a host volume (as for rivet_provider_batch.py)
  RIVET_SLACK_CONN_ID      optional: the Slack Connection (default `slack_rivet`)
  RIVET_SLACK_CHANNEL      optional: a channel id (`C…`); then the Connection is a bot token (type `slack`) and the
                           trace is a thread reply; without it the Connection is an incoming webhook (`slackwebhook`)
  RIVET_BIN                optional path of the rivet binary (default: `rivet` on PATH)

Both DAGs have no schedule: trigger them by hand. Neither reads a database nor writes a file.
  rivet_slack_test_refused    asks for an export the config does not define: refused before rivet starts
                              (`RIVET_AIRFLOW_EXPORT_UNKNOWN`), final, Airflow never retries it.
  rivet_slack_test_transient  points the source at a port nobody listens on: rivet fails with a retryable code
                              (`RIVET_PLAN_SOURCE_UNREADABLE`). `retries=0` here, so the alert comes on the first
                              try; in a real DAG keep the retries and the alert comes after the last one.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow_provider_rivet.callbacks import slack_failure_callback
from airflow_provider_rivet.dags import build_batch_dag

CONFIG = str(Path(__file__).resolve().parent / "configs" / "postgres_batch.yaml")
# with RIVET_SLACK_CHANNEL (a channel id) the Connection is a bot token and the trace goes into a thread
ALERT = slack_failure_callback(
    os.environ.get("RIVET_SLACK_CONN_ID", "slack_rivet"), channel=os.environ.get("RIVET_SLACK_CHANNEL") or None
)
OPERATOR = {"rivet_bin": os.environ.get("RIVET_BIN", "rivet")}
NOBODY = "postgresql://nobody@127.0.0.1:1/nothing"  # no secret: nothing listens on port 1

rivet_slack_test_refused = build_batch_dag(
    "rivet_slack_test_refused",
    config=CONFIG,
    state_dir=os.environ.get("RIVET_EXAMPLE_STATE_DIR"),
    exports=["no_such_export"],
    load=False,
    operator_kwargs={**OPERATOR, "env": {"RIVET_PG_URL": NOBODY}},
    default_args={"retries": 0, "on_failure_callback": ALERT},
    tags=["rivet", "slack-test"],
)

rivet_slack_test_transient = build_batch_dag(
    "rivet_slack_test_transient",
    config=CONFIG,
    state_dir=os.environ.get("RIVET_EXAMPLE_STATE_DIR"),
    exports=["users"],
    load=False,
    operator_kwargs={**OPERATOR, "env": {"RIVET_PG_URL": NOBODY}},
    default_args={"retries": 0, "on_failure_callback": ALERT},
    tags=["rivet", "slack-test"],
)
