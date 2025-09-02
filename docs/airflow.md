# Documentation sur airflow

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

### Tâches

Les tâches sont des unités de travail qui s'éxecutent via des travailleurs (workers). Elles sont arrangés dans des DAG suivant des dépendances
en amont et en aval.

Les tâches sont divisés en trois catégories :
* Les opérateurs
* Les capteurs
* Les fonctions python décoré comme des TaskFlow @task

À noter que les tâches ne se transmettent par d'information les unes aux autres. Pour débloquer cela, les XComs sont nécessaires.

## Opérateur

Les opérateurs sont les briques des workflows. Tous les opérateurs héritent de la classe `BaseOperator`. Certains opérateurs populaires sont `PythonOperator` et `BashOperator`. Il en existe plein d'autres. On peut les voir comme des templates pour des tâches.
