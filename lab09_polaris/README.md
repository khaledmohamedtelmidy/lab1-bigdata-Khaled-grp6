# Lab 9 — Gouverner les tables Iceberg avec Apache Polaris

Énoncés : `09.polaris/lab-1-deployment.md` et `09.polaris/lab-2.md` du repo `adaltas/ece-bigdata-2026-fall`.

## Contenu du dossier

| Fichier | Rôle |
|---|---|
| `deployment/` | Chart Helm Polaris + PostgreSQL, job de bootstrap RBAC, secret DB |
| `setup_polaris.sh` | Partie 1 : ressources du pod, ConfigMap, secrets, `helm upgrade --install`, récupération des credentials `dev` / `analyst` |
| `env.sh` | Variables d'environnement à sourcer avant chaque étape |
| `run_polaris.py` | Partie 2 : session Spark (catalogues `lab`, `local`, `polaris`) et une étape du lab par mode |
| `exercise2_silver_reader.sh` | Exercice 2 : catalog role `silver_reader` limité au namespace `silver` (API de management) |

> Les tables du lab 8 ont été écrites dans un warehouse **local** (`./warehouse`). Le mode `dev` les recopie dans
> `s3a://<bucket>/warehouse` (catalogue `lab`) si elles n'y sont pas, pour que `register_table` pointe vers un
> emplacement autorisé du catalogue Polaris.

## Exécution (terminal VS Code Onyxia, racine du repo)

```bash
cd /home/onyxia/work/lab1-bigdata-Khaled-grp6
git pull

# Partie 1 — déploiement (si le pod redémarre après le patch des ressources, relancer la commande)
bash lab09_polaris/setup_polaris.sh

# Partie 2 — gouvernance
source lab09_polaris/env.sh
python lab09_polaris/run_polaris.py dev         # register, create, ADD COLUMN
python lab09_polaris/run_polaris.py analyst     # lectures OK, INSERT / ALTER refusés
python lab09_polaris/run_polaris.py snapshots   # l'INSERT refusé n'a pas créé de snapshot

# Exercices
python lab09_polaris/run_polaris.py exercise1   # DROP COLUMN country
python lab09_polaris/run_polaris.py gold        # namespace gold + table de test
bash lab09_polaris/exercise2_silver_reader.sh
source lab09_polaris/env.sh
python lab09_polaris/run_polaris.py silver_reader

# Fin du lab (silver.orders reste enregistrée pour le lab Trino)
python lab09_polaris/run_polaris.py cleanup
```

Credential perdu : `curl -s -X POST "$API/principals/<nom>/reset" -H "Authorization: Bearer $TOKEN" | jq`
(token root : voir `exercise2_silver_reader.sh`).

## Rôles

| Principal | Principal role | Catalog role | Privilèges |
|---|---|---|---|
| `dev` | `data_engineer` | `catalog_contributor` | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` |
| `analyst` | `data_analyst` | `catalog_reader` | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` |
| `silver_reader` (ex. 2) | `silver_data_analyst` | `silver_reader` | `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` **sur `silver` uniquement** |

## Réponses aux questions

### SparkSession avec le catalogue Polaris

**Quel composant a répondu à chaque `SHOW NAMESPACES` ?**
- `SHOW NAMESPACES IN lab` : c'est **Spark lui-même**, via la bibliothèque Iceberg. Le catalogue Hadoop n'a pas de serveur : il liste les dossiers sous `s3a://<bucket>/warehouse/`.
- `SHOW NAMESPACES IN polaris` : c'est le **serveur Polaris**, par une requête HTTP `GET /api/catalog/v1/<catalog>/namespaces`. Il lit sa base PostgreSQL.
- Comment le voir : la liste `polaris` est vide tant qu'on n'a pas fait `CREATE NAMESPACE polaris.silver`, alors que les dossiers existent dans le bucket. Si on coupe le pod Polaris, seule la seconde commande échoue, avec une erreur de connexion HTTP.

**Où le credential est-il utilisé ?**
Dans une **requête à Polaris**. Spark envoie `clientId:clientSecret` à `/api/catalog/v1/oauth/tokens` (flux OAuth2 client credentials) et reçoit un bearer token. Ce token accompagne ensuite chaque appel au catalogue. Le credential n'est jamais envoyé au bucket : les fichiers sont lus avec les clés S3A de la session.

### Enregistrer la table Silver

**Qu'a écrit `register_table` ?**
- Dans Polaris : une entité table `silver.orders` dans PostgreSQL, qui pointe vers le fichier `metadata.json` courant.
- Dans le bucket : **rien**. Le nombre d'objets sous `warehouse/silver/orders/` est le même avant et après. Aucune donnée ni métadonnée n'est copiée.

**Pourquoi continuer à écrire via `lab` est dangereux ?**
Les deux catalogues ont chacun leur propre pointeur vers la « version courante » : `version-hint.text` pour Hadoop, la base PostgreSQL pour Polaris. Un commit via `lab` déplace seulement le pointeur Hadoop, et Polaris reste sur l'ancienne version. Les deux vues **divergent**, et un commit suivant via Polaris repart de l'ancien état : les écritures faites via `lab` disparaissent. La coordination des commits ne marche que si tout le monde passe par le même catalogue.

### Créer des tables en tant que `dev`

**Où Polaris a-t-il stocké `silver.users`, et pourquoi ?**
Sous `s3://<bucket>/warehouse/silver/users/`, c'est-à-dire `default-base-location` du catalogue + namespace + table. Le chemin est dans les `allowedLocations` du catalogue. Les fichiers metadata suivent la convention REST (`00000-<uuid>.metadata.json`) et non `vN.metadata.json` comme le catalogue Hadoop.

**Différence entre « enregistrée » et « créée via Polaris » ?**
- **Enregistrée** (`orders`) : les fichiers ont été écrits avant, via le catalogue Hadoop, à un emplacement choisi par celui-ci. Polaris ne fait que référencer le metadata existant.
- **Créée via Polaris** (`users`) : c'est Polaris qui choisit l'emplacement et qui valide chaque commit dès le premier.

Dans les deux cas, c'est **Spark** qui écrit les fichiers Parquet. Polaris n'écrit jamais de données, seulement le pointeur de métadonnées.

### Changement de schéma en tant que `dev`

**Quel fichier enregistre le changement ?**
Un **nouveau** fichier `0000N-<uuid>.metadata.json` de `silver.users`, visible dans `metadata_log_entries`. Ce n'est pas le fichier pointé par `register_table`, qui concernait `orders`. Un fichier metadata existant n'est jamais modifié : chaque commit en crée un nouveau, et Polaris déplace son pointeur dessus.

**Quel privilège Polaris vérifie-t-il ?**
`TABLE_WRITE_PROPERTIES`, nécessaire pour modifier les métadonnées d'une table (`updateTable` sans nouvelles données). `dev` l'a grâce à `CATALOG_MANAGE_CONTENT`.

### Lire en tant qu'analyst

**Quel privilège manque pour l'`INSERT` ?**
`TABLE_WRITE_DATA`. `catalog_reader` ne fournit que `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` et `CATALOG_READ_PROPERTIES`. Polaris répond `403 Forbidden` (`ForbiddenException`). L'`ALTER TABLE` échoue de la même façon, faute de `TABLE_WRITE_PROPERTIES`.

**L'`INSERT` refusé a-t-il créé un snapshot ?**
Non : `run_polaris.py snapshots` montre la même liste qu'avant. Le commit est rejeté par Polaris, donc aucun nouveau snapshot ni metadata n'est validé.

### Ce que Polaris ne peut pas imposer ici

Le catalogue est créé avec `stsUnavailable: true` : il n'y a **pas de credential vending**, et tous les principals lisent les fichiers avec les mêmes clés S3 statiques (`HadoopFileIO`). Polaris contrôle donc les opérations **sur le catalogue**, pas l'accès aux fichiers. Un analyst qui possède les clés du bucket peut lire, voire écrire, les Parquet directement avec `aws s3` ou `spark.read.parquet`. Pour fermer ce trou, il faut un stockage compatible STS : Polaris distribuerait alors des credentials temporaires limités au chemin de la table, en lecture seule pour `analyst`.

## Exercices

### Exercice 1 — supprimer la colonne `country`

`ALTER TABLE polaris.silver.users DROP COLUMN country` (mode `exercise1`).
- La liste des **snapshots ne change pas**. On voit en revanche un nouveau fichier dans `metadata_log_entries`, avec un nouveau `latest_schema_id`.
- C'est une opération de **métadonnées** : Iceberg identifie les colonnes par un ID, pas par leur nom ni leur position. Supprimer `country` revient à écrire un nouveau schéma dans un nouveau `metadata.json`. Aucun fichier Parquet n'est réécrit : les colonnes absentes du schéma courant sont simplement ignorées à la lecture.

### Exercice 2 — catalog role `silver_reader`

`exercise2_silver_reader.sh`, avec le token root et l'API de management :
1. Crée le catalog role `silver_reader`.
2. Lui donne des grants de type **`namespace`** sur `["silver"]` : `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA`. Un privilège donné sur un namespace s'applique à toutes ses tables, et seulement à elles.
3. Crée le principal role `silver_data_analyst` et lui rattache `silver_reader`.
4. Crée un principal dédié `silver_reader`, dont le credential est ajouté à `~/.polaris-credentials.env`, et lui assigne `silver_data_analyst`. Un principal séparé évite de modifier `analyst`, qui a déjà un accès en lecture à tout le catalogue.

Test (mode `gold` puis mode `silver_reader`) :
- `SHOW TABLES IN polaris.silver` et `SELECT count(*) FROM polaris.silver.orders` : **OK**.
- `SHOW TABLES IN polaris.gold` et `SELECT * FROM polaris.gold.orders_by_product` : **Forbidden**, car aucun grant n'existe sur `gold`.
- `INSERT INTO polaris.silver.orders ...` : **Forbidden**, car il manque `TABLE_WRITE_DATA`.

C'est le principe du moindre privilège appliqué aux couches médaillon : un data scientist lit `silver`, sans voir `gold` et sans pouvoir écrire.

## Nettoyage

Le mode `cleanup` supprime `polaris.silver.users`, la table de test et le namespace `gold`. `silver.orders` reste enregistrée, car le lab Trino la lit via Polaris.
