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
    pl.col('shop_status') == 'actif'
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
