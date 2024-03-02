# Création d'index

Les scripts de cette partie permettent de créer des index afin d'optimser les temps d'exécution. 

## Mesure du temps d'exécution

Oracle permet de mesurer le temps d'exécution en rajoutant deux lignes avant et après le script.

```sql
SET TIMING ON;
--<REQUETE SQL>;
SET TIMING OFF;
```

## Mesure de la cardinalité

La mesure de cardinalité d'une colonne se fait avec un script affichant le résultat sous forme de pourcentage indiquant le ratio entre les valeurs distinctes et le nombre de valeurs total d'une colonne.

Si le ratio est inférieur à 1%, alors la documentation officielle d'oracle conseille d'utiliser un index Bitmap au lieux des index B-Tree habituels.

Pour la mesure de la cardinalité de la colonne **day_of_week** de la table **dim_date** :

```sql
SELECT 
    COUNT(DISTINCT "day_of_week") AS nb_val_distinctes,
    COUNT("day_of_week") AS nb_enregistrements,
    ROUND(COUNT(DISTINCT "day_of_week") /
    COUNT("day_of_week") * 100, 2) AS "Ratio %"
FROM 
    dim_date;
```

## Création d'un index Bitmap

Si une colonne possède une mesure de cardinalité inférieure à 1% alors un index Bitmap est créé sur les colonnes. Exemple :

```sql
CREATE BITMAP INDEX idx_bitmap_date
ON dim_date_3("year", "month", "day", "day_of_week");
```

## Création d'un index B-Tree

Les index B-Tree correspondent aux index classiques qu'on retrouve sur les colonnes identité. Ils sont beaucoup moins nombreux que les index Bitmap sur les entrepôts de données.

```sql
CREATE INDEX idx_checkin
ON fact_checkin("checkin_id");
```

## ORA-01450 - Erreur clé trop longue

Parfois en créant l'index il se peut que la clé soit trop longue. Dans ce cas, il faut modifier la longueur en alterant la table.

D'abord, obtenir la taille de la colonne idéale en trouvant sa valeur la plus longue

```sql
SELECT MAX(LENGTH("business_id")) AS max_key_length
FROM dim_business;
```

Puis modifier la taille de la colonne avec cette valeur

```sql
ALTER TABLE dim_business
MODIFY "business_id" VARCHAR2(22);
```


# Vue matérialisée

La vue matérialisée permet d'enregistrer physiquement le résultat d'une requête contrairement aux vues normales qui n'enregistrent que la requête. Cela permet d'agréger certaines valeurs afin d'éviter à les recalculer à chaque fois, afin de l'exploiter à la manière d'un data cube.


## Création de la vue

On peut créer une vue sur fact_review et ses dimensions, en agrégeant la somme de toutes les mesures, ainsi que le type de l'utilisateur (Elite ou Non) dans une nouvelle colonne "user_type" afin d'éviter de faire la jointure à chaque fois.

```sql
CREATE MATERIALIZED VIEW cube_review_2
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS
SELECT
    dim_date."date_id" AS "date_id",
    dim_business."business_id" AS "business_id",
    fact_review."user_id" AS "user_id",
    dim_user."elite" AS "user_type",
    SUM(fact_review."stars_measure") AS "total_stars",
    SUM(fact_review."useful_measure") AS "total_useful",
    SUM(fact_review."funny_measure") AS "total_funny",
    SUM(fact_review."cool_measure") AS "total_cool",
    SUM(fact_review."word_count_measure") AS "total_word_count"
FROM
    fact_review
JOIN
    dim_date ON fact_review."date_id" = dim_date."date_id"
JOIN
    dim_business ON fact_review."business_id" = dim_business."business_id"
JOIN
    dim_user ON fact_review."user_id" = dim_user."user_id"
GROUP BY
    dim_date."date_id",
    dim_business."business_id",
    fact_review."user_id",
    dim_user."elite";
```

## Exploitation de la vue

La vue peut ensuite être requêtée. 

Pour obtenir la moyenne du nombre de mots, et des avis useful/funny/cool par les autres utilisateurs des critiques pour chaque type d'utilisateur :

```sql
SELECT
    dim_user."elite" AS is_elite,
    AVG("total_cool") AS avg_cool,
    AVG("total_useful") AS avg_useful,
    AVG("total_funny") AS avg_funny,
    AVG("total_word_count") AS avg_word_count
FROM
    cube_review
JOIN
    dim_user ON cube_review."user_id" = dim_user."user_id"
GROUP BY
    dim_user."elite";
```

On peut creuser la dimension temporelle (opération DRILL DOWN) en groupant sur l'année, ou bien le mois.

```sql
SELECT
    dim_date."year" AS "Annee",
    dim_user."elite" AS "Utilisateur Elite",
    AVG(total_word_count) AS "Moyenne de mots"
FROM
    cube_review
JOIN
    dim_date ON cube_review."date_id" = dim_date."date_id"
JOIN
    dim_user ON cube_review."user_id" = dim_user."user_id"
GROUP BY
    dim_date."year", dim_user."elite";
```


On peut aussi extraire un sous-cube en filtrant l'année sur une plage (opération DICE).

```sql
SELECT
    dim_date."year",
    CASE WHEN "user_type" = 1 THEN 'elite' ELSE 'non-elite' END AS "user_type",
    AVG("total_word_count") AS avg_word_count,
    AVG("total_stars") AS avg_num_reviews,
    AVG("total_cool") AS avg_cool,
    AVG("total_useful") AS avg_useful
FROM
    cube_review_2
JOIN
    dim_date ON cube_review_2."date_id" = dim_date."date_id"
WHERE
    dim_date."year" BETWEEN 2015 AND 2019
GROUP BY
    CASE WHEN "user_type" = 1 THEN 'elite' ELSE 'non-elite' END,
    dim_date."year"
ORDER BY
    dim_date."year", "user_type";
```