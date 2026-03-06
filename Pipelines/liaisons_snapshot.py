"""
Completion snapshot
"""

from Connector.connector import Connector
from Connector.db_credentials import (
    SSHCredentials, MySpaceCredentials, LisaCredentials, AnalyticsCredentials)
from Connector.queries.liasions_status import LiaisonsQuery
from Transform.transformer import Transformer
from Transform.specific_transform_functions import (
    compute_liaisons_status_snapshot,
)
from Connector.queries.liasions_status import LiaisonsQuery
from utils.generate_sql_create_table import generate_sql_create_table


def pipeline(timestamp_date_str: str):
    # ------ EXTRACT ------
    print('Starting extraction...')
    extract = Connector(
        SSHCredentials(),
        LisaCredentials(),
    ).extract_database(LiaisonsQuery.query)

    # ------ TRANSFORM ------
    transform = Transformer(extract, timestamp_date_str, compute_liaisons_status_snapshot).run()

    # ------ LOAD ------
    print('Starting loader...')
    Connector(SSHCredentials(), AnalyticsCredentials()).load(transform, 'liaisons_snapshot')


if __name__ == '__main__':
    pipeline('01-03-2026')
