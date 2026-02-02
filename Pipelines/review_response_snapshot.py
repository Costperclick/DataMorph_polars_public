"""
Completion snapshot
"""

from Connector.connector import Connector
from Connector.db_credentials import (
    SSHCredentials, MySpaceCredentials, LisaCredentials, AnalyticsCredentials)
from Connector.queries.review_reply import ReviewReplyQuery
from Transform.transformer import Transformer
from Transform.specific_transform_functions import (
    compute_reviews_response_rate_snapshot,
)
from utils.generate_sql_create_table import generate_sql_create_table


def pipeline(timestamp_date_str: str):
    # ------ EXTRACT ------
    print('Starting extraction...')
    extract = Connector(
        SSHCredentials(),
        MySpaceCredentials(),
    ).extract_database(ReviewReplyQuery.query)

    # ------ TRANSFORM ------
    transform = Transformer(extract, timestamp_date_str, compute_reviews_response_rate_snapshot).run()

    # ------ LOAD ------
    print('Starting loader...')
    Connector(SSHCredentials(), AnalyticsCredentials()).load(transform, 'review_reply_snapshot')


if __name__ == '__main__':
    pipeline('01-01-2026')
