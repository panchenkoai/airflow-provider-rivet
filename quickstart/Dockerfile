# The official Airflow image plus the rivet binary (rivet-cli) and this provider, pinned under Airflow's constraints.
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION
ARG RIVET_VERSION=0.32.2
ARG PROVIDER_VERSION=0.2.0
RUN PY=$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")') \
 && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      "rivet-cli==${RIVET_VERSION}" "airflow-provider-rivet==${PROVIDER_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt" \
 && rivet --version && python -c "import airflow_provider_rivet"
