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

#### `agency.txt`

Obligatoire. Contient les colonnes:
* [PK] `agency_id` (recommandé, obligatoire si plusieurs agences)
* `agency_name` (obligatoire)
* `agency_url` (obligatoire)
* `agency_timezone` (obligatoire)
* `agency_lang` (optionnel)
* `agency_phone` (optionnel)

Également (non présents dans notre jeu de données)
* `agency_fare_url` (optionnel)
* `agency_email` (optionnel)

#### `stops.txt`

Obligatoire sous condition. Contient les colonnes:
* [PK] `stop_id` (obligatoire)
* `stop_code` (optionnel)
* `stop_name` (obligatoire)
* `stop_lat` (obligatoire sous condition: `location_type in [0,1,2]`)
* `stop_lon` (obligatoire sous condition: `location_type in [0,1,2]`)
* `zone_id` (optionnel): zone de tarification
* `location_type` (optionnel): 0 ou Null (stop), 1 (station), 2 (entrance/exit), 3 (generic node), 4 (boarding area)
* `parent_station` (obligatoire sous condition: `location_type in [2,3,4]`, recommandé si `location_type = 0`, interdit si `location_type = 1`)
* `stop_timezone` (optionnel)
* `wheelchair_boarding` (optionnel)

Également (non présents dans notre jeu de données)
* `tts_stop_name` (optionnel): text-to-speech name
* `stop_desc` (optionnel) : description
* `stop_url` (optionnel)
* `level_id` (optionnel)
* `platform_code` (optionnel)

#### `routes.txt`

Obligatoire. Contient les colonnes:
* [PK] `route_id` (obligatoire)
* `agency_id` (obligatoire sous condition: plusieurs agences définis)
* `route_short_name` (obligatoire sous condition: `route_long_name` est vide)
* `route_long_name` (obligatoire sous condition `route_short_name` est vide)
* `route_type` (obligatoire): train, bus, ...
* `route_url` (optionnel)
* `route_color` (optionnel)
* `route_text_color` (optionnel)

Également (non présents dans notre jeu de données)
* `route_desc` (optionnel)
* `route_sort_order` (optionnel): pour la présentation aux clients
* `continuous_pickup` (interdit sous condition)
* `continuous_drop_off` (interdit sous condition)
* `networkd_id` (interdit sous condition)

#### `trips.txt`

Obligatoire. Contient les colonnes:
* [FK] `route_id` (obligatoire)
* [FK] `service_id` (obligatoire)
* [PK] `trip_id` (obligatoire)
* `trip_headsign` (optionnel)
* `trip_short_name` (optionnel)
* `direction_id` (optionnel)
* [FK] `shape_id` (obligatoire sous condition): forme géospatial du trajet du véhicule
* `wheelchair_accessible` (optionnel)
* `bikes_allowed` (optionnel)

Également (non présents dans notre jeu de données)
* `block_id` (optionnel): un bloc est un ensemble de trip
* `cars_allowed`

#### `stop_times.txt`

Obligatoire. Contient les colonnes:
* [PK,FK] `trip_id` (obligatoire)
* `arrival_time` (obligatoire sous condition. interdit sous condition)
* `departure_time` (obligatoire sous condition. interdit sous condition)
* [FK] `stop_id` (obligatoire sous condition. interdit sous condition)
* [PK] `stop_sequence` (obligatoire)
* `pickup_type` (interdit sous condition)
* `drop_off_type` (interdit sous condition)

Également (non présents dans notre jeu de données)
* [FK] `location_group_id` (interdit sous condition)
* [FK] `location_id` (interdit sous condition)
* `stop_headsign` (optionnel)
* `start_pickup_drop_off_window` (obligatoire sous condition)
* `end_pickup_drop_off_window` (obligatoire sous condition)
* `continuous_pickup` (interdit sous condition)
* `continuous_drop_off` (interdit sous condition)
* `shape_dist_traveled` (optionnel)
* `timepoint` (optionnel)
* `pickup_booking_rule_id` (optionnel)
* `drop_off_booking_rule_id` (optionnel)

#### `calendar.txt`

Obligatoire sous condition. Contient les colonnes:
* [PK] `service_id` (obligatoire)
* `monday` (obligatoire)
* `tuesday` (obligatoire)
* `wednesday` (obligatoire)
* `thursday` (obligatoire)
* `friday` (obligatoire)
* `saturday` (obligatoire)
* `sunday` (obligatoire)
* `start_date` (obligatoire)
* `end_date` (obligatoire)

#### `calendar_dates.txt`

Obligatoire sous condition. Contient les colonnes:
* [PK,FK] `service_id``(obligatoire)
* [PK] `date` (obligatoire)
* `exception_type` (obligatoire)

#### `shapes.txt`

Optionnel. Contient les colonnes:
* [PK] `shape_id` (obligatoire)
* `shape_pt_lat` (obligatoire)
* `shape_pt_lon` (obligatoire)
* [PK] `shape_pt_sequence` (obligatoire)
* `shape_dist_traveled` (optionnel)

#### `feed_info.txt`

Obligatoire sous condition. Contient les colonnes:
* `feed_publisher_name` (obligatoire)
* `feed_publisher_url` (obligatoire)
* `feed_lang` (obligatoire)
* `feed_start_date` (obligatoire)
* `feed_end_date` (obligatoire)

Également (non présents dans notre jeu de données)
* `default_lang` (optionnel)
* `feed_version` (optionnel)
* `feed_contact_email` (optionnel)
* `feed_contact_url` (optionnel)

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

Voir: https://gtfs.org/fr/documentation/realtime/reference/


## Simple et facile à utiliser

* **Facilité d’intégration:** GTFS permet aux agences de démarrer facilement avec une structure de données simple utilisant des formats de fichiers courants tels que .txt et GeoJSON, favorisant ainsi la collaboration et l’interopérabilité.
* **Rétrocompatible:** lors de la mise à jour de la spécification, les flux existants restent valides et maintiennent la compatibilité avec les analyseurs existants.

## Accès à la documentation technique

https://gtfs.org/fr/documentation/overview/