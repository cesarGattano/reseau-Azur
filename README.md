# reseau-Azur
TP de pratique d'ETL (Airflow - duckDB) sur des données au standard GTFS &amp; GTFS-RT


## Mise en place des dockers pour la première fois:

Création des répertoires nécessaires et gestion de droits

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

Création du fichier de configuration de airflow

```
docker compose run airflow-cli airflow config list
```

Initialisation de airflow
```
docker compose up airflow-init
```

Lancement de tous les conteneurs pour mise en place de l'espace de travail
```
docker compose up
```

Ensuite les DAGs peuvent être lancé via l'interface graphique accessible via `http://localhost:8080`
Connexion > login:airflow | mdp:airflow
