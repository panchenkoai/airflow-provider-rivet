# Slack alerts: setup guide

A failed rivet task posts one message to a Slack channel: a one-line title,
`dag_id.task_id — <error type>`, with the run id and a link to the task log under it. The error
text goes into that message's thread as a code block: rivet's error line for the failed step, or,
when the task failed outside rivet, the exception's message. The channel stays a list of titles.

It is Airflow's `on_failure_callback` (`slack_failure_callback` from this package) sending through
the Slack provider. rivet itself sends nothing: do not configure `notifications:` in a rivet
config run under Airflow, or every failure is sent twice.

Two ways to connect, both set up below:

| | Bot token (recommended) | Incoming webhook (fallback) |
|---|---|---|
| Channel message | title + run id + task-log link | title + run id + task-log link + error text |
| Error text | a reply in the thread, a code block | in the same message |
| Airflow Connection type | `slack` ("Slack API") | `slackwebhook` ("Slack Incoming Webhook") |
| Password field holds | the bot token, `xoxb-…` | the whole URL, `https://hooks.slack.com/services/…` |
| DAG names | the conn_id and the channel id: `slack_failure_callback("slack_rivet", channel="C…")` | the conn_id: `slack_failure_callback("slack_rivet_webhook")` |

A webhook cannot thread: Slack answers it with `ok`, not with the message id a reply needs.

## 1. Install the Slack provider

On every worker, and wherever the scheduler or DAG processor parses DAGs:

```bash
pip install "airflow-provider-rivet[slack]"
```

or `apache-airflow-providers-slack` with Airflow's constraints file. The official Airflow images
already ship it.

## 2. Create the Slack app

<https://api.slack.com/apps> → *Create New App* → *From scratch* → a name (it is the sender's
name in the channel, e.g. `rivet-airflow`) and your workspace → *Create App*.

### Bot token (recommended)

1. *Features* → *OAuth & Permissions* → *Scopes* → *Bot Token Scopes* → *Add an OAuth Scope*:
   `chat:write`. It is the only scope the callback uses.
2. Top of the same page: *Install to Workspace* → *Allow*. **After adding a scope to an installed
   app, click *Reinstall to Workspace***: until then the token keeps its old scopes, and Slack
   answers `missing_scope` (see Troubleshooting).
3. Copy the *Bot User OAuth Token*, `xoxb-…` (step 3 below says where to put it).
4. In the channel: `/invite @rivet-airflow` (your app's name). A bot posts only where it is a
   member; a private channel works the same way.
5. The channel id: click the channel's name → *About* (or *Channel details*) → at the bottom,
   `Channel ID: C…` → copy. The callback needs the id, not the name: the token has no
   `channels:read` scope, so the bot cannot look a name up, and a name fails with
   `channel_not_found`.

### Incoming webhook (fallback, no thread)

*Features* → *Incoming Webhooks* → *Activate Incoming Webhooks*: On → *Add New Webhook to
Workspace* → pick the channel → *Allow*. Copy the *Webhook URL*. A webhook posts to that one
channel only.

## 3. Store the secret

**The token, and the webhook URL, are credentials**: whoever has one posts into your channels.
Store them like a password: never in a DAG file, a repository, a ticket, a chat, or a command
line (argv is visible to every user of the host in `ps`, and lands in shell history).

Getting it from the browser into a file without pasting it into a terminal or a chat: copy it
in Slack, then on a Mac

```bash
mkdir -p ~/.config/rivet
(umask 077; pbpaste > ~/.config/rivet/slack_bot_token)
wc -c < ~/.config/rivet/slack_bot_token    # a bot token is about 55-60 bytes; nothing is printed
```

(on Linux `xclip -o -selection clipboard` or `wl-paste` instead of `pbpaste`). The file is mode
`0600` from the start. The steps below read it and never print it.

Where the Connection lives decides who else can read it. The reasoning, and the same choice for
the database and warehouse Connections, is in the README section "Connections and secrets:
recommended setup" (added by #21).

**Recommended: a secrets backend** (Google Secret Manager, AWS Secrets Manager, Vault). With
Airflow's Secret Manager backend configured as in that section (`connections_prefix:
airflow-connections`, `sep: -`), the conn_id `slack_rivet` is the secret
`airflow-connections-slack_rivet`:

```bash
jq -Rs '{conn_type: "slack", password: rtrimstr("\n")}' < ~/.config/rivet/slack_bot_token |
  gcloud secrets create airflow-connections-slack_rivet --data-file=-
```

For a webhook: `conn_type: "slackwebhook"`, the URL file instead of the token file.

**Acceptable: the metadata database, with a Fernet key set** (`[core] fernet_key`). Check that
there is one; this prints a length, not the key:

```bash
airflow config get-value core fernet_key | wc -c    # more than 1
```

Without a key Airflow stores the token in clear text. Then either the UI (*Admin* →
*Connections* → *+*: Connection Id `slack_rivet`, Connection Type *Slack API*, the token in
*Slack API Token*; for a webhook *Slack Incoming Webhook*, the URL in *Webhook URL* or
*Password*), or a file only you can read:

```bash
(umask 077; jq -Rs '{slack_rivet: {conn_type: "slack", password: rtrimstr("\n")}}' \
  < ~/.config/rivet/slack_bot_token > /tmp/slack_rivet.json)
airflow connections import /tmp/slack_rivet.json && rm /tmp/slack_rivet.json
```

Not `airflow connections add --conn-password <token>`: that puts the token in argv. In a
container, copy the JSON file in and run the import there.

**Never in production: `AIRFLOW_CONN_SLACK_RIVET`** in the environment, or the token in DAG code.
An environment variable is readable through `docker inspect`, `/proc/<pid>/environ` of every
Airflow process, compose files and deployment manifests.

The callback resolves the Connection on the worker, in the process of the task that failed, so
the backend must be configured for the workers. On Airflow 3 a worker asks its own configured
backends first and then the API server, which reads the metadata database and the API server's
backends.

## 4. Wire the callback

Put it in the DAG's `default_args`; the builders merge it over their `retries=2`:

```python
from airflow_provider_rivet.callbacks import slack_failure_callback
from airflow_provider_rivet.dags import build_batch_dag

dag = build_batch_dag(
    "rivet_postgres",
    config="/opt/rivet/configs/postgres.yaml",
    state_dir="/opt/rivet/state",
    exports=["users", "orders"],
    default_args={"retries": 2, "on_failure_callback": slack_failure_callback("slack_rivet", channel="C0123ABCDEF")},
)
```

For a webhook, leave `channel` out: `slack_failure_callback("slack_rivet_webhook")`. Other keyword
arguments go to the Slack hook (`timeout=`, `proxy=`). An operator you build yourself takes the
same callback:

```python
from airflow_provider_rivet.callbacks import slack_failure_callback
from airflow_provider_rivet.operators import RivetRunOperator

run = RivetRunOperator(
    task_id="run_users",
    config="/opt/rivet/configs/postgres.yaml",
    export="users",
    state_dir="/opt/rivet/state",
    on_failure_callback=slack_failure_callback("slack_rivet", channel="C0123ABCDEF"),
)
```

The builders' `watcher` task carries no failure callback, so a failed run sends one message per
failed task and nothing for the watcher.

Airflow 2.10 and 3.x behave the same for this callback: it runs in the failed task's own process
on the worker, after the task has failed for good, with the exception in its context. The
differences are where the link points and what the log line looks like:

- The task-log link uses `[webserver] base_url` on Airflow 2 and `[api] base_url` on Airflow 3.
  Unset, it points at `http://localhost:8080`: set it to the address your users open.
- An error in the callback is logged as `Error in callback at index 0: notify` on Airflow 2 and
  `Failed to run task callback` on Airflow 3.

## 5. What arrives

Airflow calls `on_failure_callback` only when a task has failed for good: its last try failed,
or it failed in a way Airflow does not retry. A try that Airflow will retry sends nothing. So
every message is about a task that is failed now; the title says what kind of failure it is.

The error type is the rivet code with `final` or `retryable`; with several failed exports, the
first one's code and `+N more`; when the task died without a rivet result, the exception's class
(`AirflowTaskTimeout`, ...).

**Final** (a refusal, or a stop before rivet started): running it again changes nothing; fix what
the code names. An export the config does not have:

> :x: **rivet_slack_test.apply.no_such_export — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)**
> run `manual__2026-10-16T10:57:40+00:00` · task log

In its thread, one code block with the exception's message (the task stopped before rivet ran):

```text
[RIVET_AIRFLOW_EXPORT_UNKNOWN] config /opt/rivet/configs/postgres_batch.yaml has no export `no_such_export`
```

**Retryable**: the cause may go away by itself, and the retries ran out. Once it is fixed (here:
the database is back), clear the task:

> :x: **rivet_slack_test.apply.users — RIVET_PLAN_SOURCE_UNREADABLE (retryable)**
> run `manual__2026-10-16T10:57:40+00:00` · task log

In its thread, rivet's own error line for the step that failed (read from the end of that step's
stderr file on the worker):

```text
[RIVET_PLAN_SOURCE_UNREADABLE] plan 'users': the source could not be read, so no plan was written (`rivet apply` would fail on it): nothing is listening on 127.0.0.1:1 — check the port in `url:` and that the server is up (driver: error connecting to server: Connection refused (os error 111))
```

The thread holds no Python traceback and no other stderr line: they are in the task log and in
the stderr file, which the task log names.

**What the thread can hold.** Unlike XCom and the task log (README, "Secrets"), the thread
carries rivet's error text, and that text can hold source values (a cell that failed to
convert). The callback passes it through Airflow's secrets masker: every Connection password and
masked value the task saw becomes `***` (rivet already removes credentials from its own error
text). A value from the data is not a secret Airflow knows: send alerts to a channel whose
members may read the source data. A short password is masked wherever its characters appear,
also inside words. The text is cut at 3,500 characters (3,000 through a webhook), ending in
`… truncated, full log: <task log link>`.

## 6. Test it with a DAG that fails on purpose

`examples/rivet_slack_alerts.py` defines two DAGs with no schedule that read no database and
write no file: `rivet_slack_test_refused` (an export the config does not have: final) and
`rivet_slack_test_transient` (a source on a port nobody listens on: retryable). Both have
`retries=0`, so the alert comes on the first try.

1. Copy `examples/rivet_slack_alerts.py` and `examples/configs/` into the DAGs folder (the DAG
   reads `configs/postgres_batch.yaml` next to itself).
2. On the workers and the scheduler: `RIVET_EXAMPLE_STATE_DIR` (an existing directory),
   `RIVET_SLACK_CHANNEL` (the channel id; leave it unset for a webhook) and
   `RIVET_SLACK_CONN_ID` if the conn_id is not `slack_rivet`. None of them is a secret.
3. Trigger both:

```bash
airflow dags unpause rivet_slack_test_refused && airflow dags trigger rivet_slack_test_refused
airflow dags unpause rivet_slack_test_transient && airflow dags trigger rivet_slack_test_transient
```

Within seconds each run fails, and the channel gets two titles,
`rivet_slack_test_refused.apply.no_such_export — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)` and
`rivet_slack_test_transient.apply.users — RIVET_PLAN_SOURCE_UNREADABLE (retryable)`, each with a
thread reply holding its error text (section 5). The failed task's log says what was sent, after the failure:

```text
Slack alert for rivet_slack_test_refused.apply.no_such_export posted to channel C0123ABCDEF (ts 1760612345.000100) through connection `slack_rivet`, its error text in the thread
```

(through a webhook:
``Slack alert for ... sent through connection `slack_rivet_webhook`: HTTP 200 ok``). Pause or
delete the two test DAGs when done.

## 7. Troubleshooting

Slack's refusal is raised in the callback and Airflow logs it with the error name (step 4 says
which line). If neither that nor the `Slack alert for ...` line is in the failed task's log, the
callback never ran: check that `on_failure_callback` reached the task (`default_args` of that
DAG, the DAG file the worker parsed).

| Slack says | Meaning | Fix |
|---|---|---|
| `not_in_channel` | the bot is not a member of the channel | `/invite @<app>` in the channel |
| `channel_not_found` | a channel name instead of its id, a typo, or a private channel the bot is not in | the `C…` id from *About*; invite the bot |
| `missing_scope` (`needed: chat:write`) | the token lacks the scope | add it under *Bot Token Scopes*, then *Reinstall to Workspace* |
| `invalid_auth`, `not_authed`, `token_revoked`, `account_inactive` | the Connection's token is wrong, empty, or revoked (an app removed or reinstalled elsewhere) | copy the current *Bot User OAuth Token* into the Connection again |
| ``The conn_id `...` isn't defined`` | no backend the worker reads has that Connection | step 3, for the workers |
| webhook `404 no_service` | the webhook was removed, or the app uninstalled | a new webhook URL (step 2) |
| webhook `403 invalid_token` / `action_prohibited` | the webhook belongs to a revoked app, or an admin restricted posting | a new webhook URL; ask the workspace admin |
| webhook `404 channel_not_found` / `410 channel_is_archived` | the webhook's channel was deleted or archived | a new webhook for a live channel |
| a timeout, a proxy or DNS error | the worker cannot reach `slack.com` / `hooks.slack.com` | open egress to them, or pass `proxy=` |
