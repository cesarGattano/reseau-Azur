# Documentation sur airflow

Airflow est une plateforme qui permet de construire et faire tourner des flots de travail (workflows). Un flot de travail est représenté par un DAG contenant différentes tâches arrangés suivant un système de dépendances et de flot de données à prendre en compte.

## Définition de notions clés de airflow

### DAG

Le DAG est un modèle qui encapsule tout ce qui est nécessaire pour éxecuter un flot de travail (workflow).
Il peut être modéliser comme une séquence de tâches dépendant les unes des autres (sans cycle possible).
Par exemple :
- a >> b (b dépend de a)
- a >> c (c dépend de a)
- [b,c] >> d (d dépend de b et c)

Un DAG peut être planifié (quand le DAG doit s'éxecuter).

Un DAG peut appelé des Callbacks lorsqu'il a finit de travailler avec succès.

D'autres possibilités sont bien évidemment offertes pour aller plus loin.

À noter que le terme DAG vient de la notion mathématique Directed Acyclic Graph (voir le brief neo4j).

Les Dags sont conçus pour être exécuter une grand nombre de fois et plusieurs éxecutions peuvent s'effectuer en parallèle.

### Tâches

Les tâches sont des unités de travail qui s'éxecutent via des travailleurs (workers). Elles sont arrangés dans des DAG suivant des dépendances
en amont et en aval.

Les tâches sont divisés en trois catégories :
* Les opérateurs
* Les capteurs
* Les fonctions python décoré comme des TaskFlow @task

À noter que les tâches ne se transmettent par d'information les unes aux autres. Pour débloquer cela, les XComs sont nécessaires.

#### Opérateur (operators)

Les opérateurs sont les briques des workflows. Tous les opérateurs héritent de la classe `BaseOperator`. Certains opérateurs populaires sont `PythonOperator` et `BashOperator`. Il en existe plein d'autres. On peut les voir comme des templates pour des tâches.

#### Capteurs (sensors)

Ce sont des opérateurs speciaux qui sont conçus pour une seule chose: attendre que quelque chose survienne. Cela peut-être basé sur le temps, ou l'attente d'un fichier ou n'importe quel évenement extérieur. Lorsque ce quelque chose est réalisé, ils donnent la main aux tâches qui dépendent de ce capteur.

Il existe deux modes:
* poke: le capteur réquisitionne un travailleur pour l'entiereté de son éxecution
* reschedule: le capteur réquisitionne un travailleur uniquement lorsqu'il analyse puis s'endort pour une certaine durée avant la prochaine analyse.

En général c'est une question de latence, une vérification toutes les secondes seraient plutôt en mode poke, tandis que toutes les minutes seraient en mode reschedule.

#### Fonction python décoré avec le TaskFlow @task

Des fonctions python personnalisées empaquetés comme des tâches.

### Les XComs (Cross-communications)

C'est un mécanisme où les tâches peuvent pousser et tirer une petite quantité de metadata. Un XCom est identifié par une clé. Ils sont conçus pour de petite quantité de données. Ne pas passer de dataframe à travers eux.


## Les rôles dans airflow

* Deployment managers: responsable de l'installation de la sécurité et de la configuration de airflow
* Authenticated UI users: utilisateurs pouvant accéder à l'UI et l'API airflow pour interagir
* Dag authors: responsable de la création et de la soumission des DAGs a airflow

## Les composants de airflow

### Le plannificateur (scheduler)

Il contrôle les DAGs et les tâches. Il déclenche les instances de tâches dont les dépendences ont été réalisés. En arrière-plan un processus tourne et reste synchro avec tous les DAGs. Une fois par minute (par défaut), il analyse et vérifie si chaque tâche active peut être déclenchée.

La plannificateur est conçu pour être persistant dans environnement de travail. Il s'éxecute grâce à la commande airflow scheduler

### Les exécuteurs (executor)

Les exécuteurs sont des mécanismes à partir desquels des instances de tâches sont exécutés.

#### Exécuteur local

Exécution de code sur la machine où le scheduler tourne. Cela signifie que le processus du scheduler peut être affecté et potentiellement affecté l'installation de Airflow.

Pros: facile à utiliser, rapide, latence faible et peu de prérequis.
Cons: Sécurité faible, limité en capacités et partage de ressource avec le scheduler

#### Exécuteur distant (Queued/Batch)

Les tâches sont envoyés dans une file d'attente central à partir de laquelle les travailleurs distants vont finir les tirer pour les exécuter.

Pros: plus robuste (scheduler et worker sont decouplés). Travail en parallèle par les workers.
Cons: problème de gestion des ressources entre les travailleurs en fonction de la charge de travail.

* CeleryExecutor
* BatchExecutor

##### Exécuteur Celery

Exécution de code sur des Celery workers. Cela signifie qu'il peut influencer toutes les tâches qui sont exécuter sur le même worker. Pas d'isolation entre les tâches à moins que les Cluster Policies sépare l'exécution des tâches par queues.

Sécurité moyenne.

Celery travaille de pair avec Redis (ou un autre Celery Backend). L'appli web flower est aussi dédié au controle des travailleurs.

#### Exécuteur distant (Conteneurisé)

Les tâches sont exécutés dans des conteneurs. Elles sont isolés

Pros: pas de problème de gestion de ressources. Environnement customisable par tâche. Les travailleurs n'ont une durée de vie que pour l'exécution de la tache
Cons: latence au démarrage. Peu être couteuse dans le cas d'exécution de nombreuses petites tâches. Besoin de gérer un groupe Kubernetes

* KubernetesExecutor
* EcsExecutor

#####  Exécuteur Kubernetes

Exécution de code sur des POD Kubernetes. Isolation des tâches assurée.

Sécurité forte

### L'analyseur DAG (DAG processor)

Il analyse les fichiers DAG et les sérialise dans la base de données metadata.

L'analyse des fichiers DAG est la lecture des fichiers python qui les définissent et les enregistre pour qu'il soit plannifiable par le scheduler.

Il existe deux composantes couvrant l'analyse des DAGs:
* Le Manager: boucle infini qui détermine quels fichiers sont à analyser
* La Process: processus séparé qui convertir un fichier python en un objet DAG

Le manager est appelé par la commande `airflow dag-processor`

### Le serveur web

Il fournit une interface graphique utilisateur pour inspecter, lancer ou debuger le comportement des DAGs et des tâches.

### Le dossier contenant les fichiers DAGs

Il est lu par le plannificateur pour savoir quelles tâches peuvent être lancées et quand les lancer.

### Une base de données metadata

Elle est utilisé pour conserver l'état des workflows et des tâches. Mettre en place une telle base de données est nécessaire pour que airflow fonctionne. Cette base de données est géré par sqlAlchemy. Une database backend avec PostgreSQL ou MySQL est nécessaire. SQLite est utilisé par défaut pour une intention de développement uniquement.

Dans le cas du docker compose cette étape est réalisé dans le conteneur `airflow-init`

### Les travailleurs (workers)

Ils éxecutent les tâches données par le plannificateur. Dans le cas de CeleryExecutor, ils sont des long processus qui tournent en continu. Dans le cas de KubernetesExecutor, ils sont des POD

Un travailleur dans le cas de CeleryExecutor est appelé via la commande `airflow celery worker`

### Le déclencheur (triggerer)

Il exécute les tâches différables dans un boucle d'événements asyncio.

Les opérateurs différables peut se suspendre lorsqu'il n'a rien d'autre à faire qu'attendre pour libérer un travailleur. Lorsqu'un opérateur diffère, son éxecution est envoyée au déclencheur.

## Astuces

### Les groupes de tâches

Lorsque les DAGs deviennent complexes, les groupes de tâches peuvent être utiles. Ils permettent d'organiser les tâches sous forme visuelles voir de répéter un pattern le long du DAG.