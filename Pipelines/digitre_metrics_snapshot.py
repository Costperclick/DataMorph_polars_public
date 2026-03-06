import polars as pl
from Connector.db_credentials import (SSHCredentials, AnalyticsCredentials, LisaCredentials)
from Connector.connector import Connector
from Connector.queries.completion_and_fields import QueryShopCompletionScore


query_completion = """
SELECT *
FROM completion_snapshot
WHERE franchise_label = 'Optimhome2025';
"""

query_review_reply = """
SELECT *
FROM review_reply_snapshot
WHERE franchise_label = 'Optimhome2025';
"""

query_score = """
SELECT 
    dpr.erp_shop_id, 
    dpr.shop_label, 
    dpr.page_id,
    dpr.date, 
    dpr.rating, 
    dpr.rating_count, 
    dpr.nb_replies, 
    dpr.media,
    lmt.franchise_id,
    lmt.franchise_label
FROM daily_pages_reviews AS dpr
LEFT JOIN lookup_media_tools AS lmt
    ON dpr.erp_shop_id = lmt.erp_shop_id
WHERE lmt.franchise_label = 'optimhome2025'
  AND dpr.media = 'gmb'
ORDER BY dpr.date DESC;
"""

query_gbp_metrics = """
SELECT 
    lmt.erp_shop_id,
    lmt.shop_label,
    lmt.franchise_label,
    IF(lm.inactive, 'résilié', 'actif') AS shop_status,
    gppr.page_id,
    DATE_FORMAT(gppr.date,'%m-%Y') AS date,
    SUM(business_impressions_desktop_maps) AS impressions_desktop_maps,
    SUM(business_impressions_desktop_search) AS impressions_desktop_search,
    SUM(business_impressions_desktop) AS impressions_desktop_total,
    SUM(business_impressions_mobile_maps) AS impressions_mobile_maps,
    SUM(business_impressions_mobile_search) AS impressions_mobile_search,
    SUM(business_impressions_mobile) AS impressions_mobile_total,
    SUM(business_impressions) AS impressions_total,
    SUM(business_conversations) AS conversations,
    SUM(business_direction_requests) AS direction_requests,
    SUM(call_clicks) AS call_clicks,
    SUM(website_clicks) AS website_clicks,
    SUM(clicks) AS clicks_total,
    SUM(business_bookings) AS bookings,
    SUM(business_food_orders) AS food_orders,
    SUM(actions_total) AS actions_total
FROM google_pages_performance_report gppr
INNER JOIN lookup_myspace lm ON lm.gmb_page_id = gppr.page_id
INNER JOIN lookup_media_tools lmt ON lmt.erp_shop_id = lm.erp_shop_id
WHERE lmt.franchise_label = 'optimhome2025'
  AND gppr.date >= '2026-01-01'
GROUP BY lmt.erp_shop_id, lmt.shop_label, lmt.franchise_label, IF(lm.inactive, 'résilié', 'actif'), gppr.page_id, DATE_FORMAT(gppr.date,'%m-%Y')
ORDER BY lmt.shop_label, gppr.date;
"""

# Recupère le snapshot de completion
completion_metrics = Connector(SSHCredentials(), AnalyticsCredentials()).extract_database(query_completion)
completion_df = completion_metrics.filter(
    pl.col('shop_status').is_not_null()
).select(
    pl.col('period'),
    pl.col('shop_id'),
    pl.col('shop_label'),
    pl.col('shop_status'),
    pl.col('franchise_label'),
    pl.col('completion_rate')
)

# Recupère le snapshot de taux de réponse, nombre d'avis, nombre d'avis répondu.
review_reply_metrics = Connector(SSHCredentials(), AnalyticsCredentials()).extract_database(query_review_reply)
review_reply_df = review_reply_metrics.select(
    pl.col('date'),
    pl.col('erp_shop_id'),
    pl.col('total_reviews'),
    pl.col('answered_reviews'),
    pl.col('response_rate'),
)

# recupère la note sur la période
score_metrics = Connector(SSHCredentials(), LisaCredentials()).extract_database(query_score)
# Passe en string l'id pour merge later
score_df = score_metrics.with_columns([
    pl.col('erp_shop_id').cast(pl.Int64),
]).select(
    pl.col('date'),
    pl.col('erp_shop_id'),
    pl.col('rating'),
    pl.col('page_id')
)


gbp_metrics = Connector(SSHCredentials(), LisaCredentials()).extract_database(query_gbp_metrics)
gbp_metrics_df = gbp_metrics.with_columns(
    pl.col('date').str.strptime(pl.Date, "%m-%Y"), # date arrive en string depuis gbp_metrics
    pl.col('erp_shop_id').cast(pl.Int64),
).select(
    pl.col('erp_shop_id'),
    pl.col('page_id'),
    pl.col('date'),
    pl.col('impressions_total'),
    pl.col('actions_total'),
    pl.col('call_clicks'),
    pl.col('website_clicks'),
)

optimhome_final_merge = completion_df.join(
    review_reply_df, left_on=['period', 'shop_id'], right_on=['date', 'erp_shop_id'], how='left'
).join(
    score_df, left_on=['period', 'shop_id'], right_on=['date', 'erp_shop_id'], how='left'
).join(
    gbp_metrics_df, left_on=['period', 'shop_id'], right_on=['date', 'erp_shop_id'], how='left'
)

optimhome_final_merge.write_csv('optimhome_final_merge.csv')

optimhome_pre_compute_df = optimhome_final_merge.with_columns(
    pl.when((pl.col('rating') < 4.2) & (pl.col('total_reviews') < 50))
      .then(pl.lit("orange"))
    .when((pl.col('rating') < 4.2) & (pl.col('total_reviews') >= 50))
      .then(pl.lit("red"))
    .when((pl.col('rating') >= 4.2) & (pl.col('total_reviews') < 50))
      .then(pl.lit("grey"))
    .otherwise(pl.lit("green"))
    .alias("color_category")
)

optimhome_pre_compute_df = optimhome_pre_compute_df.with_columns([
    pl.col('impressions_total').cast(pl.Float64),
    pl.col('website_clicks').cast(pl.Float64),
    pl.col('call_clicks').cast(pl.Float64),
    pl.col('actions_total').cast(pl.Float64),
    # Crée une colonne scalaire pour le taux de réponse
    ((pl.when(pl.col('total_reviews') > 0)
         .then(pl.col('answered_reviews') / pl.col('total_reviews') * 100)
         .otherwise(0))
     .round(2)
     .alias("taux_de_reponse")
    )
])

# 2️⃣ Agrégations par période
optimhome_full_compute = optimhome_pre_compute_df.filter(
    pl.col('shop_status') == "actif",
).with_columns([
    pl.col('rating').cast(pl.Float64),
    pl.col('completion_rate').cast(pl.Float64),
    pl.col('total_reviews').cast(pl.Float64),
    pl.col('taux_de_reponse').cast(pl.Float64),
    pl.col('impressions_total').cast(pl.Float64),
    pl.col('actions_total').cast(pl.Float64),
    pl.col('website_clicks').cast(pl.Float64),
    pl.col('call_clicks').cast(pl.Float64)
]).group_by('period').agg(
    (pl.col('shop_status') == "actif").cast(pl.Int32).sum().alias("Nombre de shops actifs"),
    pl.mean('completion_rate').round(2).alias('taux de complétion moyen'),
    pl.mean('total_reviews').round(2).alias("Nombre d'avis moyen"),
    pl.mean('rating').round(2).alias("Note moyenne"),
    pl.mean('taux_de_reponse').round(2).alias('taux de réponse'),
    # Comptages conditionnels
    (pl.col('color_category') == "red").cast(pl.Int32).sum().alias('Nb de fiche dans la zone rouge'),
    (pl.col('color_category') == "orange").cast(pl.Int32).sum().alias('Nb de fiche dans la zone orange'),
    (pl.col('color_category') == "grey").cast(pl.Int32).sum().alias('Nb de fiche dans la zone grise'),
    (pl.col('color_category') == "green").cast(pl.Int32).sum().alias('Nb de fiche dans la zone verte'),
    # Actions et clics
    pl.sum('impressions_total').alias("Nb d'impressions globales"),
    pl.mean('impressions_total').round(2).alias("Nb d'impressions moyen"),
    pl.sum('actions_total').alias("Nb d'actions globales"),
    pl.mean('actions_total').round(2).alias("Nb d'actions moyen"),
    pl.sum('website_clicks').alias("Nb de clic vers site web"),
    pl.mean('website_clicks').round(2).alias("Nb moyen de clic vers site web"),
    pl.sum('call_clicks').alias("Nombre de clic sur appel"),
    pl.mean('call_clicks').round(2).alias("Nombre moyen de clic sur appel"),
)
for name, dtype in zip(optimhome_full_compute.columns, optimhome_full_compute.dtypes):
    print(name, dtype)


print(optimhome_full_compute)
optimhome_full_compute.write_csv('test_optimhome_full_compute.csv')