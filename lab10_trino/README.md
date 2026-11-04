# Lab 10 — Trino : déploiement, silver → gold, star schema

Énoncés : `10.trino/lab-1-deployment.md`, `lab-2-trino-gold.md`, `lab-3-trino-star-schema.md` (repo `adaltas/ece-bigdata-2026-fall`).

## Contenu

| Fichier | Rôle |
|---|---|
| `deployment/` | Chart Helm Trino 480, coordinateur seul, catalogue `lakehouse` branché sur Polaris (REST + OAuth2, principal `dev`) |
| `setup_trino.sh` | Lab 10-1 : secrets `trino-polaris-oauth` et `polaris-s3`, configmap, `helm upgrade --install`, vérification `/v1/info` |
| `lab10_spark.py` | Spark : `users` (re)crée `polaris.silver.users` depuis `bronze/users.csv` avec l'adresse découpée ; `compare` = exercice 5 |
| `trino_lab.py` | Client Python Trino : `gold` (lab 10-2), `exercises` (exercices 1-4), `star` (lab 10-3) |
| `outputs/` | Sorties de chaque étape |
| `../lab_trino/gold/` | Métadonnées Iceberg des tables gold (commit demandé par le lab 10-3, sans les fichiers de données) |

> **Pourquoi recréer `silver.users` ?** Le nettoyage du lab 9 supprime `polaris.silver.users`, et la table `users`
> du lab 8 n'avait pas les colonnes `street`, `city`, `state` et `zip_code` utilisées par le lab 10. `lab10_spark.py users`
> reconstruit la table comme dans le lab Spark (lab 7), directement dans Polaris.

## Exécution

```bash
cd /home/onyxia/work/lab1-bigdata-Khaled-grp6
# 0. Profil AWS à jour : coller le script "AWS CLI" d'Onyxia s'il a expiré
bash lab10_trino/setup_trino.sh
source lab09_polaris/env.sh
python lab10_trino/lab10_spark.py users
uv add trino pandas
export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER=$KUBERNETES_NAMESPACE
uv run python lab10_trino/trino_lab.py gold
uv run python lab10_trino/trino_lab.py exercises
python lab10_trino/lab10_spark.py compare
uv run python lab10_trino/trino_lab.py star
aws s3 --profile default sync "s3://$KUBERNETES_NAMESPACE/warehouse/gold" ./lab_trino/gold/ --exclude "*/data/*"
```

## Réponses — lab 10-2 (silver → gold)

### Environnement
**Que change `catalog=` dans `connect()` ?** Il fixe le catalogue par défaut de la session. Une table écrite sans préfixe de catalogue (`silver.orders`) est résolue dans `lakehouse`. Les noms complets `lakehouse.silver.orders` restent valides, et `system.runtime.queries` reste accessible en le préfixant.

### Catalogues, schémas, tables
**Quel type Trino pour `ordered_at`, avec fuseau ?** `timestamp(6) with time zone`. Spark écrit son `TimestampType` comme un `timestamptz` Iceberg (instant UTC). Trino le lit avec le fuseau, affiché en UTC.

**Et `order_id` ?** `varchar`. Le type est porté par le schéma Iceberg (`string`), pas par le moteur : Trino a un type `uuid`, mais il ne l'applique pas à une colonne stockée en string.

### Interroger la couche silver
**Explication des requêtes.** Les comptages donnent le volume de la couche silver. La répartition par `state` montre le parsing d'adresse du lab 7 : les adresses militaires (`AE`, `AP`, `AA`, de forme `Unit … Box …` / `APO AE 12345`) n'ont pas de ville, donc `missing_city` vaut le nombre d'utilisateurs de ces états. La requête orpheline compte les commandes dont le `user_id` n'existe pas dans `users` : 0 attendu, sinon une jointure interne les ferait disparaître sans prévenir.

**Quel test dbt remplace-t-elle ?** Le test `relationships` de `stg_orders.user_id` vers `stg_users.user_id`. Pour le lancer à chaque rafraîchissement, on le garde comme test dbt (`dbt test`, avec l'adaptateur `dbt-trino`) ou comme assertion `orphan_orders == 0` dans le job qui construit le gold, et on bloque la publication si elle échoue.

**Pas de job ni de session : quand choisir Trino ?** Trino est un service SQL déjà démarré (coordinateur + workers longue durée) : une requête répond en quelques secondes, sans démarrer de JVM ni de contexte Spark. Il convient à l'interactif, au BI et aux transformations SQL de taille raisonnable. Spark reste préférable pour les traitements lourds, longs, tolérants aux pannes, ou qui ne s'expriment pas en SQL (Python, ML).

### Métadonnées Iceberg
**Combien de snapshots pour `silver.orders`, écrits par qui ?** Voir `outputs/gold.log` (`orders$snapshots`). Il y a le snapshot `append` écrit par **Spark** (catalogue Hadoop, lab 8, puis copie dans le bucket au lab 9). L'exercice 3 en ajoute un écrit par **Trino**.

**`aws s3 ls` contre `$files` : lequel n'a pas besoin du bucket ?** `$files` ne lit que les métadonnées (manifests) que Trino localise par Polaris. Il donne exactement les fichiers **vivants** du snapshot courant, avec leurs statistiques. `aws s3 ls` liste le bucket directement (il faut les clés S3) et montre aussi les fichiers qui ne font plus partie de la table (anciens snapshots, écritures échouées, fichiers orphelins).

### Plan et exécution
**Quelle table est diffusée (broadcast), et pourquoi ?** `users`, la petite table (environ 50 lignes) : la jointure est `REPLICATED`. Copier 50 lignes vers chaque tâche coûte moins cher que repartitionner les milliers de commandes sur le réseau.

**Où est l'équivalent du shuffle ?** Dans les **exchanges** entre fragments (`RemoteExchange` / `REPARTITION`) : l'agrégation `GROUP BY state` est découpée en agrégation partielle, puis redistribution par hachage de la clé et agrégation finale. Pour une fonction de fenêtre (`rank() OVER`), on retrouve un exchange de type `GATHER` ou repartition avant l'opérateur `Window`.

**Quelles stats regarder en premier pour une requête lente ?** Par fragment et par opérateur : les lignes et octets en entrée (`Input:`) pour repérer un scan trop large ou un filtre non poussé, le temps CPU et le temps passé à attendre (`Blocked`), la mémoire de pointe, puis la répartition des données entre les tâches (déséquilibre). Le tout en comparant les lignes estimées aux lignes réelles.

### Couche gold
**`LEFT JOIN` pour `user_activity`, jointure interne pour `orders_by_state_month` ?** `user_activity` part des utilisateurs et doit garder ceux qui n'ont jamais commandé (0 commande). `orders_by_state_month` part des commandes et a besoin du `state` de l'utilisateur. Une jointure interne y élimine les commandes orphelines : c'est elle qui dépend du résultat de la requête orpheline.

**`current_date` et le rafraîchissement ?** `age` change avec le temps : la table n'est correcte que le jour de son calcul. Il faut la reconstruire régulièrement (au moins tous les jours) ou stocker `birthdate` et calculer l'âge au moment de la lecture (dans une vue).

**Où vivent les fichiers gold, qui a choisi ?** Dans `s3://<bucket>/warehouse/gold/<table>-<uuid>/` (données et métadonnées). C'est Polaris qui a fixé l'emplacement, à partir de la `default-base-location` du catalogue et du namespace. Trino n'a pas donné de `location`.

### Validation
**Quel défaut est détecté, lequel passe inaperçu ?** Les assertions détectent une perte ou une duplication de lignes au passage silver → gold : commandes orphelines (absentes de la jointure interne, donc `units` différent), utilisateurs dupliqués dans `users` (commandes comptées deux fois). Elles ne détectent pas une valeur fausse mais conservée en total : mauvais `state` à cause du parsing, date décalée dans un autre mois, produit mal orthographié qui crée une deuxième ligne, quantités négatives qui se compensent.

**Si la première assertion échoue (commande sans utilisateur) : bug gold ou silver ?** Silver. Le gold applique correctement sa règle (jointure interne). C'est la couche silver qui viole l'intégrité référentielle (commande sans utilisateur), et il faut la corriger ou la mettre en quarantaine en amont.

### Rafraîchissement
**Snapshots après le refresh ?** Trois : celui du CTAS (`append`), celui du `DELETE` (`delete`, ou `overwrite` selon le mode : suppression de fichiers entiers), puis l'`INSERT` (`append`). Voir `outputs/gold.log`.

**Que voit un dashboard entre le `DELETE` et l'`INSERT` ? Et avec `MERGE` ?** Il lit le snapshot courant, donc une table **vide**. Chaque instruction est atomique, mais les deux ensemble ne forment pas une transaction. Un `MERGE` (ou un `CREATE OR REPLACE TABLE … AS`) produit un seul commit : le lecteur voit l'ancien état ou le nouveau, jamais un état intermédiaire.

**Comparaison avec le Parquet du lab Spark écrasé pendant une lecture ?** Un `mode("overwrite")` sur un dossier Parquet supprime puis réécrit des fichiers : un lecteur peut obtenir un mélange, des fichiers manquants ou une erreur `FileNotFound`. Avec Iceberg, les anciens fichiers restent référencés par l'ancien snapshot. Un lecteur en cours continue sur sa version, et le time travel permet de la relire.

### Comparaison avec Spark
**Ce qu'il faut à chaque moteur pour `top_products` ?** Trino : un client (`trino` en Python ou JDBC) et l'adresse du coordinateur. Catalogue, credentials Polaris et S3 sont configurés une fois côté serveur. Spark : une SparkSession par job (JVM driver), le package `iceberg-spark-runtime`, la configuration du catalogue REST avec le credential Polaris et les clés S3 dans chaque session, puis un job soumis.

**Ce qu'ils partagent, ce qu'ils ignorent l'un de l'autre ?** Ils partagent le format de table (Iceberg : métadonnées et Parquet dans le bucket) et le catalogue (Polaris, qui garde le pointeur courant et coordonne les commits). Ils ne savent rien du moteur qui a écrit la table, de ses sessions, de ses caches ou de son plan : la seule interface entre eux est le commit Iceberg.

**Trino ou Spark ?**
- Dashboard rafraîchi chaque minute sur le gold → **Trino** (faible latence, service permanent, SQL).
- Transformation nocturne de 10 To de bronze → **Spark** (traitement par lots massif, tolérance aux pannes par tâche, réessais).
- Data scientist qui explore la couche silver → **Trino** pour du SQL ad hoc. Spark (notebook) si l'exploration continue en Python ou en ML.

**Pourquoi l'absence de tolérance aux pannes est acceptable ici ?** Les requêtes du lab durent quelques secondes : si un worker tombe, on relance, pour un coût négligeable. Une transformation de 10 To dure des heures, et tout recommencer pour la perte d'un nœud coûte trop cher. Il faut l'exécution tolérante aux pannes de Trino (`retry-policy=TASK` avec stockage d'échange) ou Spark.

## Réponses — exercices (communs aux labs 10-2 et 10-3)

1. **Partitionnement `month(month)`** : la table partitionnée a un fichier (au moins) par mois, contre un seul fichier pour la table non partitionnée. Avec un filtre sur un mois, `EXPLAIN ANALYZE` montre que la table partitionnée ne lit que les lignes de cette partition (élagage par les métadonnées de partition), alors que la table non partitionnée lit tout le fichier puis filtre. Voir `outputs/exercises.log`.
2. **`gold.cohorts`** : par décennie de naissance, le nombre d'utilisateurs, de commandes et d'unités moyennes par commande (`LEFT JOIN` pour garder les utilisateurs sans commande).
3. **INSERT + MERGE + time travel** : une commande est ajoutée à `silver.orders`. Le `MERGE` ne met à jour que la ligne `state × mois` concernée (ou l'insère) en un seul snapshot. `FOR VERSION AS OF <snapshot avant>` relit l'ancien total d'unités, et l'état courant contient les nouvelles unités.
4. **`optimize`** : réécrit les petits fichiers (un par `INSERT`, `DELETE`…) en moins de fichiers plus gros. Cela crée un snapshot `replace` : les lignes ne changent pas, seule la disposition physique change. Les anciens fichiers restent référencés par les anciens snapshots jusqu'à `expire_snapshots` puis `remove_orphan_files`.
5. **Spark contre Trino** : `lab10_spark.py compare` calcule la même agrégation dans Spark via Polaris. Il obtient le même nombre de lignes et le même total d'unités que la table écrite par Trino (`EXCEPT` vide).

## Réponses — lab 10-3 (star schema)

**Ces contrôles suffisent-ils ?** Non. Ils vérifient le nombre de lignes, le total des quantités et l'existence des clés, mais pas que chaque commande pointe vers la **bonne** ligne de dimension. Par exemple, un `user_key` décalé après une reconstruction de `dim_user` (clés `row_number()` renumérotées) passerait tous les tests. Contrôles à ajouter : unicité de `order_id` dans le fait et de `user_id` dans `dim_user`, comparaison commande par commande `fact ⋈ dim_user` contre `silver.orders.user_id`, cohérence `dim_date.full_date = CAST(ordered_at AS date)`, aucun `user_key` ni `product_key` NULL. Les trois derniers sont dans `trino_lab.py star` (« Contrôles supplémentaires »).

**Grain de `fact_orders` ? De `dim_date` ?** Une ligne par commande. Une ligne par jour calendaire.

**Doublon de `user_id` dans `silver.users` ? Commande sans produit (`NULL`) ?** Un doublon donne deux lignes `dim_user` pour le même `user_id`. Le `LEFT JOIN` duplique alors les commandes : `silver_orders == fact_orders` échoue (et `silver_units == fact_units` aussi). Pour un produit `NULL`, la jointure `p.product = o.product` ne trouve rien et `product_key` vaut NULL : c'est l'assertion `unmatched == 0` qui échoue.

**`row_number()` et reconstruction de `dim_user` ?** Un nouvel utilisateur inséré dans l'ordre des `user_id` décale les `user_key` suivants. Les anciennes lignes de `fact_orders` pointeraient vers d'autres utilisateurs : il faut reconstruire le fait à chaque fois, ou utiliser des clés stables (hachage de `user_id`, séquence persistée, `MERGE` qui n'attribue des clés qu'aux nouveaux).

**Un utilisateur qui déménage ?** `dim_user` est écrasée (SCD de type 1) : les commandes passées affichent le **nouvel** état. Pour garder l'historique, il faut une dimension à évolution lente de type 2 : une ligne par version de l'utilisateur, avec `valid_from`, `valid_to`, `is_current` et une clé substitut par version, et le fait doit pointer vers la version valide à la date de commande.

**`orders_by_state_month` contre le star ?** La table agrégée répond très vite à une seule question (state × mois), sans jointure, mais toute nouvelle question demande une nouvelle table. Le star répond à n'importe quelle combinaison de dimensions (année × produit, week-end contre semaine, sexe…) sans nouvelle table, au prix de jointures et d'une agrégation au moment de la requête, sur le grain le plus fin.

## Résultats obtenus (exécution du 4 novembre 2026)

Sorties complètes dans `outputs/` : `setup_trino.log`, `spark_users.log`, `gold.log`, `exercises.log`, `spark_compare.log` et `star.log`.

| Étape | Résultat |
|---|---|
| Déploiement | Trino 480 en coordinateur seul, `HTTP 200` sur `/v1/info`, catalogue `lakehouse` relié à Polaris |
| `silver.users` | Recréée dans Polaris depuis `bronze/users.csv` : **50** utilisateurs, avec `street`, `city`, `state` et `zip_code` |
| Silver via Trino | **2 832** commandes, **50** utilisateurs, **0** commande orpheline |
| Validation gold | 4/4 OK : unités silver = gold = top_products = **8 626**, users 50 = 50, commandes 2 832 = 2 832 |
| Refresh `top_products` | Snapshots `append` (CTAS), `delete`, `append` (INSERT). Le time travel sur le premier snapshot relit l'état initial |
| Exercice 1 | Non partitionnée : 1 fichier, 50 lignes. Partitionnée `month(month)` : 4 fichiers (un par mois). Avec un filtre sur un mois, `EXPLAIN ANALYZE` lit **50 lignes** (66 % filtrées) contre **17 lignes** (0 % filtrées) : élagage de partitions |
| Exercice 2 | `gold.cohorts` : une ligne par décennie de naissance (à partir de 1910), environ 3 unités par commande par cohorte |
| Exercice 3 | Une commande ajoutée à `silver.orders` (7 unités). `MERGE` : 50 → **51** lignes et 8 626 → **8 633** unités (nouveau mois). `FOR VERSION AS OF` relit 50 lignes et 8 626 unités |
| Exercice 4 | `optimize` sur `top_products` : 1 seul fichier vivant après le refresh (le DELETE a supprimé l'ancien), donc rien à compacter. Même nombre de fichiers et d'octets avant et après |
| Exercice 5 | Spark via Polaris : 50 lignes et 8 626 unités, identiques à la table gold écrite par Trino (`EXCEPT` vide) |
| Star schema | 3/3 OK : `fact_orders` 2 833 = silver 2 833 (commande de l'exercice 3 incluse), unités 8 633 = 8 633, 0 ligne de fait sans dimension |
| Commit lab 10-3 | `lab_trino/gold/` : métadonnées Iceberg des tables gold (sans `data/`) |

### Incident mémoire (quota du namespace)

Le quota Onyxia du namespace est de **4 Gi** en `limits.memory`. Avec le pod VS Code à 2 500 Mi (lab 9), Polaris et PostgreSQL, 3 268 Mi étaient déjà pris : le pod Trino (1 Gi) était refusé (`exceeded quota: onyxia-quota`). Un essai à 820 Mi a fini en `OOMKilled`, car la JVM a besoin de mémoire hors tas. Solution retenue, celle du lab : le **patch du pod vscode-pyspark à 2 000 Mi** (`lab-1-deployment.md`, prérequis), puis Trino avec ses valeurs d'origine (1 Gi). Le déploiement `trino-coordinator` a aussi été passé en stratégie `Recreate`, pour qu'une mise à jour ne lance pas deux pods en même temps sous le quota.
