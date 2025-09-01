# Documentation sur le standard GTFS (General Transit Feed Specification)

GTFS est une norme ouverte pilotée par la communauté pour les informations de transport destinées aux usagers. Il existe deux formats de données sous une seule norme

* GTFS Schedule
* GTFS Realtime

GTFS est aujourd'hui le standard incontournable dans le monde, suivant le MDIP (Mobility Data Interoperability Principles https://www.interoperablemobility.org/definitions/#)

 L’évolution des spécifications GTFS est supervisée par une organisation indépendante à but non lucratif appelée MobilityData (https://mobilitydata.org/) et ses modifications sont guidées par les principes de facilité d’utilisation, rétrocompatibilité et changement pragmatique.


## GTFS Schedule

Contient des données statiques

* Horaires et arrêts
* Horaires et fréquences
* Tarifs
* Routes flexibles
* etc...

présentées dans une collection de fichiers textes simples cotnenus dans un seul ZIP. Dans sa forme la plus basique :
`agency.txt`, `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`, `calendar.txt` et `calendar_dates.txt`.

À noter qu'il existe un fichier spécifique geoJSON pour représenter des zones géographiques.


### Fonctionnalités des base de GTFS Schedule

* Agence: responsable du service de transport en commun
* Arrêts: où un service de transport en commun prend et dépose des passagers
* Lignes: définit les éléments d'un itinéraire de transit
* Dates de service: structure pour planifier les déplacements et les exemptions de service
* Trajets: représente les véhicules de transport en commun circulant le long d'un itinéraire défine à des heures programmées.
* Horaires d'arrêt: définie les heures d'arrivée et de départ de chaque trajet pour chaque arrêt

### Fonctionnalités complémentaires de GTFS Schédule

* Informations sur le flux
* Tracé des lignes
* Couleurs des lignes
* Vélo autorisé
* Girouette: signalisation utilisée par les véhicules indiquant la destination du voyage
* Types d'emplacement: Zone clés dans les gares de transport en commun telles que les entrées et sorties
* Fréquences: services qui fonctionnent sur une fréquence régulière ou des intervalles spécifiques
* Transferts: autorisés entre différents services de transport en commun
* Traductions: informations de service dans plusieurs langues
* Attributions: qui est impliqué par la création des données

### Accessibilité

* Accessibilité aux Personnes en Fauteuil Roulant aux Arrêts
* Accessibilité aux Personnes en Fauteuil Roulant lors des trajets
* Synthèse Vocale

### Tarifs

GTFS peut modéliser diverses structures tarifaires, telles que les tarifs basés sur la zone, la distance ou l’heure de la journée. Il informe les voyageurs des prix des trajets et des modes de paiement.

### Parcours

Les fonctionnalités de Parcours permettent de modéliser de grandes gares de transport en commun, afin que les usagers soient guidés depuis les entrées jusqu’aux zones d’embarquement. Ils fournissent des détails sur le chemin, les temps de navigation estimés et les systèmes d’orientation.

* Connexions du parcours: les points pertinents dans une station de transport en commun
* Détails du parcours
* Niveaux
* Temps de parcours en station
* Signalisation du parcours

### Services flexibles

* Arrêts continus
* Règles de réservation
* Itinéraires prédéfinis avec détours
* Services à la demande basés sur les zones
* Services à la demande avec arrêts fixes


## GTFS Realtime

Contient des données dynamiques prenant en charge les types d'informations suivantes:
* Mise à jour des trajets (retards, annulation, itinéraires modifiés)
* Alertes de service (arrêt déplacé, événements affectant une gare, un itinéraire ou l'ensemble du réseau)
* Position des véhicules (y crompis le niveau de congestion (embouteillages ?))

présentées sous le format Protocol Buffers (https://protobuf.dev/) qui est un mécanisme neutre en termes de langage et de plate-forme pour sérialiser des données structurées (pensez à XML, mais plus petit, plus rapide et plus simple).

Cette partie fonctionne en conjonction avec GTFS Schedule.

GTFS Realtime est une spécification de flux qui permet aux agences de transports publics de fournir des informations à jour sur les heures d’arrivée et de départ actuelles, les alertes de service et la position du véhicule, permettant aux utilisateurs de planifier en douceur leurs déplacements. 

### Package python disponible

Le package python GTFS-realtime-bindings (https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md) fournit des classes python générée suivant les spécifications du Buffer Protocol du repo GTFS-realtime (https://github.com/google/transit/tree/master/gtfs-realtime). Ces classes permettent de traiter des données sous forme de Protocol Buffer GTFS-RT binaire au sein des projets python.



## Simple et facile à utiliser

* **Facilité d’intégration:** GTFS permet aux agences de démarrer facilement avec une structure de données simple utilisant des formats de fichiers courants tels que .txt et GeoJSON, favorisant ainsi la collaboration et l’interopérabilité.
* **Rétrocompatible:** lors de la mise à jour de la spécification, les flux existants restent valides et maintiennent la compatibilité avec les analyseurs existants.

## Accès à la documentation technique

https://gtfs.org/fr/documentation/overview/