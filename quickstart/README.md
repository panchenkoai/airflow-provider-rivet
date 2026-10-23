# rivet + Airflow quickstart

Airflow with rivet and airflow-provider-rivet, demo MySQL and PostgreSQL sources, MinIO and ClickHouse: four CDC and
batch pipelines running in a few minutes, on Airflow 2.10 or 3.3.

```bash
cd airflow-3.3                      # or airflow-2.10
cp ../.env.example .env
docker compose up -d --build --wait # then http://localhost:8080, airflow / airflow
```

The guide: [docs/quickstart.md](../docs/quickstart.md). Your own database:
[the command builder](https://panchenkoai.github.io/airflow-provider-rivet/your-database.html).
