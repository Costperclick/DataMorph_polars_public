# Méthodologie de construction des métriques Optimhome

## 1. Extraction des données

Quatre sources de données sont extraites depuis différentes bases.

---

## 1.1 Snapshot de complétion des fiches

Source : `completion_snapshot` (base Analytics)

Filtre :
- `franchise_label = 'Optimhome2025'`

Colonnes conservées :
- `period`
- `shop_id`
- `shop_label`
- `shop_status`
- `franchise_label`
- `completion_rate`

Transformation :
- suppression des lignes où `shop_status` est `NULL`

Structure finale :

completion_df  
(period, shop_id, shop_label, shop_status, franchise_label, completion_rate)

---

## 1.2 Snapshot avis et taux de réponse

Source : `review_reply_snapshot` (base Analytics)

Filtre :
- `franchise_label = 'Optimhome2025'`

Colonnes conservées :

- `date`
- `erp_shop_id`
- `total_reviews`
- `answered_reviews`
- `response_rate`

Structure finale :

review_reply_df  
(date, erp_shop_id, total_reviews, answered_reviews, response_rate)

---

## 1.3 Snapshot notes Google

Source : `daily_pages_reviews` (base Lisa)

Jointure SQL :

daily_pages_reviews dpr  
LEFT JOIN lookup_media_tools lmt  
ON dpr.erp_shop_id = lmt.erp_shop_id  

Filtres :
- `lmt.franchise_label = 'optimhome2025'`
- `dpr.media = 'gmb'`

Colonnes conservées :

- `date`
- `erp_shop_id`
- `rating`
- `page_id`

Transformation :

- `erp_shop_id` cast en `Int64` pour homogénéiser les clés de jointure.

Structure finale :

score_df  
(date, erp_shop_id, rating, page_id)

---

## 1.4 Metrics Google Business Profile

Source : `google_pages_performance_report`

Jointures SQL :

google_pages_performance_report gppr  

INNER JOIN lookup_myspace lm  
ON lm.gmb_page_id = gppr.page_id  

INNER JOIN lookup_media_tools lmt  
ON lmt.erp_shop_id = lm.erp_shop_id  

Filtres :

- `lmt.franchise_label = 'optimhome2025'`
- `gppr.date >= '2026-01-01'`

Agrégations SQL :

Sommes sur les métriques d'impressions et d'actions.

Colonnes retenues dans le dataframe :

- `erp_shop_id`
- `page_id`
- `date`
- `impressions_total`
- `actions_total`
- `call_clicks`
- `website_clicks`

Transformations :

- `date` convertie de string `%m-%Y` vers `Date`
- `erp_shop_id` cast en `Int64`

Structure finale :

gbp_metrics_df  
(erp_shop_id, page_id, date, impressions_total, actions_total, call_clicks, website_clicks)

---

# 2. Construction du dataset principal

Toutes les sources sont jointes sur :

(period, shop_id)  
↔  
(date, erp_shop_id)

Jointures réalisées :

completion_df  

LEFT JOIN review_reply_df  
ON (period = date AND shop_id = erp_shop_id)

LEFT JOIN score_df  
ON (period = date AND shop_id = erp_shop_id)

LEFT JOIN gbp_metrics_df  
ON (period = date AND shop_id = erp_shop_id)

Type de jointure :

LEFT JOIN

Justification :

Le snapshot de **complétion** est utilisé comme **table de référence**, garantissant que toutes les fiches présentes dans ce snapshot restent dans le dataset final.

Structure résultante :

optimhome_final_merge

Export intermédiaire :

optimhome_final_merge.csv

---

# 3. Création des variables dérivées

## 3.1 Catégorisation des fiches

Une variable `color_category` est calculée selon :

| Condition | Catégorie |
|---|---|
| rating < 4.2 AND total_reviews < 50 | orange |
| rating < 4.2 AND total_reviews ≥ 50 | red |
| rating ≥ 4.2 AND total_reviews < 50 | grey |
| rating ≥ 4.2 AND total_reviews ≥ 50 | green |

---

## 3.2 Calcul du taux de réponse

Création de la colonne :

taux_de_reponse

Formule :

answered_reviews / total_reviews * 100

Gestion des divisions par zéro :

si total_reviews = 0 → taux_de_reponse = 0

Arrondi :

2 décimales

---

## 3.3 Normalisation des types

Conversion en `Float64` pour :

- `impressions_total`
- `website_clicks`
- `call_clicks`
- `actions_total`

---

# 4. Agrégation finale

Filtre préalable :

shop_status = "actif"

Agrégation :

GROUP BY period

---

## 4.1 Indicateurs structurels

- Nombre de shops actifs
- Taux de complétion moyen

---

## 4.2 Indicateurs réputation

- Nombre moyen d'avis
- Note moyenne
- Taux de réponse moyen

---

## 4.3 Distribution des catégories

Comptage conditionnel :

- nombre de fiches **red**
- nombre de fiches **orange**
- nombre de fiches **grey**
- nombre de fiches **green**

---

## 4.4 Performance GBP

### Impressions

- somme des impressions
- moyenne des impressions

### Actions

- somme des actions
- moyenne des actions

### Clics site web

- somme des clics
- moyenne des clics

### Clics appel

- somme des clics
- moyenne des clics

---

# 5. Dataset final

Structure :

optimhome_full_compute

Granularité :

1 ligne = 1 période

Export :

test_optimhome_full_compute.csv