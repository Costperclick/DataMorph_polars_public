"""
Completion snapshot
"""

from Connector.connector import Connector
from Connector.db_credentials import (MySpaceCredentials, SSHCredentials, AnalyticsCredentials)
from Connector.queries.liasions_status import LiaisonsQuery
from Transform.transformer import Transformer
from Transform.specific_transform_functions import (
    compute_liaisons_status_snapshot,
)
from Connector.queries.review_reply import


def pipeline(timestamp_date_str: str):
    print('Starting extraction...')
    extract = Connector(
        SSHCredentials(),
        MySpaceCredentials()
        ).extract_database(LiaisonsQuery.query)
    print('Starting transformation...')
    transform = Transformer(extract, timestamp_date_str, compute_liaisons_status_snapshot).run()
    print('Starting loader...')
    load = Connector(SSHCredentials(), AnalyticsCredentials()).load(transform, 'completion_snapshot')


if __name__ == '__main__':
    pipeline('01-01-2026')
