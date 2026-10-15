# The official Airflow image with the rivet binary under test and this provider from the checkout.
ARG AIRFLOW_IMAGE
FROM ${AIRFLOW_IMAGE}
USER root
COPY rivet /usr/local/bin/rivet
RUN chmod 0755 /usr/local/bin/rivet && rivet --version \
 && python -m venv /opt/snow && /opt/snow/bin/pip install --no-cache-dir -q snowflake-cli==3.29.0 \
 && ln -s /opt/snow/bin/snow /usr/local/bin/snow && snow --version
USER airflow
COPY --chown=airflow:root provider /opt/airflow-provider-rivet
RUN pip install --no-cache-dir --no-deps /opt/airflow-provider-rivet && python -c "import airflow_provider_rivet"
