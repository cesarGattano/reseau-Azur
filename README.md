# reseau-Azur
TP de pratique d'ETL (Airflow - duckDB) sur des données au standard GTFS &amp; GTFS-RT



Installation



```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" >> .env
docker compose run airflow-cli airflow config list
docker compose up airflow-init
```