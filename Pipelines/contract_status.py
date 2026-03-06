"""
Stores contract status of shops
"""

from Connector.connector import Connector
from Connector.db_credentials import (MySpaceCredentials, SSHCredentials, AnalyticsCredentials)
from Transform.transformer import Transformer
from Transform.specific_transform_functions import (
    does_shop_has_licence,
)
from Connector.queries.completion_and_fields import QueryShopCompletionScore


def pipeline(timestamp_date_str:str):
    print('Starting extraction...')
    extract = Connector(SSHCredentials(), MySpaceCredentials().extract_database(
        QueryShopContractStatus.query,
    ))
    print('Starting transformation...')
    transform = Transformer(extract, timestamp_date_str, specific_function).run()
    print('Starting loading...')
    load = Connecter(SSHCredentials(), AnalyticsCredentials()).load(transform, 'table_name')


if __name__ == '__main__':
    pipeline('your_date')