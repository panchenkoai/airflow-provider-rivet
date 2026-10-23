-- rivet's state, one database per pipeline: a pipeline's cursors, run journal and load ledger never meet another's.
CREATE DATABASE rivet_state_mysql_cdc OWNER rivet;
CREATE DATABASE rivet_state_mysql_batch OWNER rivet;
CREATE DATABASE rivet_state_postgres_cdc OWNER rivet;
CREATE DATABASE rivet_state_postgres_batch OWNER rivet;
