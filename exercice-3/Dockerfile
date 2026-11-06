# Image commune à Airflow, Jupyter et au site de reporting : Airflow + DuckDB (lecture S3, écriture PostgreSQL)
# + JupyterLab + Plotly (graphiques). FastAPI et uvicorn (site web) sont fournis par Airflow 3.
# requests et boto3 (téléchargement des sources, dépôt dans le bucket) sont déjà fournis par l'image Airflow.
FROM apache/airflow:3.3.2

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" duckdb==1.5.* jupyterlab plotly==7.1.*

# Extensions DuckDB téléchargées à la construction plutôt qu'à chaque exécution
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL postgres')"
