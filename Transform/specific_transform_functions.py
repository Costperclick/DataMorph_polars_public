"""
A list of specific functions for transformations.
"""

import polars as pl
from Transform.utils_transform_functions import timestamp_dataframe


# ========================================
# ------------- COMPLETION ---------------
# ========================================

def completion_and_fields_snapshot(df: pl.DataFrame, timestamp_date_str: str) -> pl.DataFrame:
    df = (df.pipe(timestamp_dataframe, date_str='01-01-2026')
    .filter(
        pl.col("shop_id").is_not_null()
    ).select(
        pl.col('date').alias('period'),
        pl.col(['shop_id', 'shop_label', 'franchise_label', 'shop_status']),
        pl.col('completion_rate').cast(int),
        pl.col('display_name').is_not_null().alias('has_display_name'),
        pl.col('google_description').is_not_null().alias('has_google_description'),
        pl.col('facebook_description').is_not_null().alias('has_facebook_description'),
        pl.col('services').is_not_null().alias('has_services'),
        pl.col('has_service_description'),
        pl.col('categories_primary_category').is_not_null().alias('has_categories'),
        pl.col('has_additional_categories'),
        pl.col('opening_date').is_not_null().alias('has_opening_date'),
        pl.col('phone').is_not_null().alias('has_phone'),
        pl.col('street').is_not_null().alias('has_street'),
        pl.col('city').is_not_null().alias('has_city'),
        pl.col('zip_code').is_not_null().alias('has_zip_code'),
        pl.col('country_code').is_not_null().alias('has_country_code'),
        pl.col('email_address').is_not_null().alias('has_email'),
        pl.col('service_areas').is_not_null().alias('has_service_areas'),
        pl.col('website_url').is_not_null().alias('has_website_url'),
        pl.col('opening_hours').is_not_null().alias('has_hours'),
        pl.col('special_hours').is_not_null().alias('has_special_hours'),
        pl.col('more_hours').is_not_null().alias('has_more_hours'),
        pl.col('has_facebook').is_not_null(),
        pl.col('has_instagram').is_not_null(),
        pl.col('has_linkedin').is_not_null(),
        pl.col('has_pinterest').is_not_null(),
        pl.col('has_x').is_not_null(),
        pl.col('has_logo').is_not_null(),
        pl.col('has_cover').is_not_null()
    ))
    return df


# ==========================================
# --------------- REVIEWS ------------------
# ==========================================

def compute_reviews_response_rate_snapshot(df: pl.DataFrame, timestamp_date_str:str='01-01-2026') -> pl.DataFrame:
    """
    Builds a snapshot of the amount of reviews, amount of replied reviews, review response rate by shop

    :param df: a pl.DataFrame.
    :param timestamp_date_str: a string containing the timestamp date (%d-%m-%Y format).
    :return: a timestamped snapshot of the review response metrics by shop.
    """
    df = (df.pipe(timestamp_dataframe, date_str=timestamp_date_str)
    .group_by("erp_shop_id")
    .agg(
        pl.col(['date', 'shop_label', 'franchise_label', 'shop_status']).first(),
        total_reviews=pl.col('erp_shop_id').len(),
        answered_reviews=pl.col('review_reply').is_not_null().sum()
    ).with_columns(
        response_rate=(pl.col("answered_reviews") / pl.col("total_reviews")
                       * 100
                       ).round(2)
    )
    )
    return df


# ==========================================
# --------------- LIAISONS -----------------
# ==========================================

def compute_liaisons_status_snapshot(df: pl.DataFrame, timestamp_date_string: str) -> pl.DataFrame:
    """
    Build a snapshot of the liaisons status metrics by shop.
    :param df: a pl.DataFrame.
    :param timestamp_date_str: a string containing the timestamp date (%d-%m-%Y format).
    :return: a timestamped snapshot of the liaisons by shop.
    """
    df = (df.pipe(timestamp_dataframe, date_str=timestamp_date_string)
    .filter(
        pl.col('franchise_id').is_not_null()
    ).select(
        pl.col(['date', 'erp_shop_id', 'shop_label', 'franchise_id']),
        pl.col('statut_du_shop').alias('shop_status'),
        pl.col('facebook_page_id').is_not_null().cast(int).alias('has_fb'),
        pl.col('gmb_page_id').is_not_null().cast(int).alias('has_gmb'),
        pl.col('instagram_page_id').is_not_null().cast(int).alias('has_insta'),
        pl.col('linkedin_organization_page_id').is_not_null().cast(int).alias('has_lkdin_page'),
        pl.col('linkedin_profile_page_id').is_not_null().cast(int).alias('has_lkdin_profil'),
    ).with_columns(
        total_page=pl.sum_horizontal('has_fb', 'has_insta', 'has_lkdin_page', 'has_lkdin_profil')
    ))
    return df
