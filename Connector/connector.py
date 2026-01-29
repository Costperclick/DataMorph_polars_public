import pandas
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, text
import polars as pl
import pandas as pd

class Connector:
    """
    A class gathering all information required to connect and query the database.
    Use the `.extract_database()` to extract and .load() to insert.

    :param ssh: an instance of the SSHCredentials class from /Extract/credentials.py
    :param database: an instance of the DataBaseCredentials from /Extract/credentials.py

    Example:
        >>> ssh_creds = SSHCredentials()
        >>> db_creds = DBCredentials()
        >>> Connector.extract_database(query="SELECT * FROM table")
    """
    def __init__(self,
                 ssh: 'type[Infos]',
                 database: 'type[Infos]'):
        # === SSH credentials ===
        self.ssh_host = ssh.host
        self.ssh_port = ssh.port
        self.ssh_user = ssh.username
        self.ssh_key_path = ssh.key_path
        self.ssh_key_password = ssh.key_password
        # === DataBase credentials ===
        self.db_url = database.url
        self.db_host = database.host
        self.db_username = database.username
        self.db_password = database.password
        self.db_port = database.port
        self.db_name = database.name

    @staticmethod
    def generate_engine_url(
            dialect: str = '', driver: str = '',
            username: str = '', password: str = '',
            host: str = '', port: int = 3306, database: str = '') -> str:
        """
        Builds the complete SQLAlchemy URL.
        Exemple : mysql+pymysql://lisa:monpassword@localhost:3306/lisa_db?charset=utf8mb4
        """
        url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/"
        if database:
            url += database
        url += "?charset=utf8mb4"
        return url


    def extract_database(self, query:str, schema_overrides:dict=None) -> pl.DataFrame:
        """
        Opens the SSH tunnel, connects and queries the database passing the query as text().

        :param query: the SQL query to execute.
        :param schema_overrides: dictionary enforcing dtype for specific columns.
        :return: the query results as pl.DataFrame.
        """
        try:
            with SSHTunnelForwarder(
                    (self.ssh_host, self.ssh_port),
                    ssh_username=self.ssh_user,
                    ssh_pkey=self.ssh_key_path,
                    remote_bind_address=(self.db_host, self.db_port),
                    local_bind_address=('127.0.0.1', 0)
            ) as tunnel:

                dynamic_port = tunnel.local_bind_port
                print(f'Extract pipeline - Tunnel ouvert sur le port dynamique : {dynamic_port}')

                actual_engine_url = self.generate_engine_url(
                    dialect='mysql',
                    driver='pymysql',
                    username=self.db_username,
                    password=self.db_password,
                    host='127.0.0.1',
                    port=dynamic_port,
                    database=self.db_name
                )

                try:
                    engine = create_engine(actual_engine_url, echo=True)
                    with engine.connect() as conn:
                        result = conn.execute(text(query))
                        panda_bridge = pandas.DataFrame(result.fetchall())
                        df = pl.from_pandas(panda_bridge)
                        return df
                        #return pl.read_database(query, connection=engine, schema_overrides=schema_overrides)

                except Exception as e:
                    print(f'Error during engine connection: {e}')
                    raise

        except Exception as e:
            print(f'SSH tunnel failed: {e}')
            raise


    def load(self, data: pl.DataFrame, table_name: str) -> None:
        """
        Loads / Write datas on the database's table.

        :param data: the pl.DataFrame containing the data
        :param table_name: the name of the targeted table.
        """
        try:
            with SSHTunnelForwarder(
                    (self.ssh_host, self.ssh_port),
                    ssh_username=self.ssh_user,
                    ssh_pkey=self.ssh_key_path,
                    ssh_private_key_password=self.ssh_key_password,
                    remote_bind_address=(self.db_host, self.db_port),
                    local_bind_address=('127.0.0.1', 0)  # Port 0 = Port dynamique safe
            ) as tunnel:

                actual_engine_url = self.generate_engine_url(
                    dialect='mysql', driver='pymysql',
                    username=self.db_username, password=self.db_password,
                    host='127.0.0.1', port=tunnel.local_bind_port,
                    database=self.db_name
                )

                engine = create_engine(actual_engine_url)
                data.write_database(
                    table_name=table_name,
                    connection=engine,
                    if_table_exists="append",
                    engine="sqlalchemy"
                )
                print(f"Load pipeline COMPLETED - {len(data)} rows inserted !")

        except Exception as e:
            print(f'Load pipeline ERROR : {e}')
            raise
