"""
Completion snapshot
"""

from Connector.connector import Connector
from Connector.db_credentials import (MySpaceCredentials, SSHCredentials, AnalyticsCredentials)
from Transform.transformer import Transformer
from Transform.specific_transform_functions import (
    completion_and_fields_snapshot,
)
from Connector.queries.completion_and_fields import QueryShopCompletionScore


def pipeline(timestamp_date_str:str):
    print('Starting extraction...')
    extract = Connector(SSHCredentials(), MySpaceCredentials()).extract_database(
        QueryShopCompletionScore.query,
        schema_overrides={'zip_code':str}
    )
    print('Starting transformation...')
    transform = Transformer(extract,timestamp_date_str, completion_and_fields_snapshot).run()
    print('Starting loader...')
    load = Connector(SSHCredentials(), AnalyticsCredentials()).load(transform, 'completion_snapshot')


if __name__ == '__main__':
    pipeline('01-01-2026')