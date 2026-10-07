# Image commune à Airflow et Jupyter : Airflow + DuckDB (lecture S3, écriture PostgreSQL) + JupyterLab + matplotlib
FROM apache/airflow:3.3.2

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" duckdb==1.5.* jupyterlab matplotlib

# Extensions DuckDB téléchargées à la construction plutôt qu'à chaque exécution
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL postgres')"
