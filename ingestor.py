import pandas as pd
from time import time


class ParquetIngestor:
    """
    Class for ingesting data from parquet files into a Postgres SQL database.

    Args:
        db_connection: SQLAlchemy engine or connection string for the database
        file: Data location to be consumed
        table_name: The name for the table to be created in the Database

    """

    def __init__(self, file, db_connection, table_name):
        """
        Constructor for ParquetIngestor class.

        Args:
            db_connection: SQLAlchemy engine or connection string for the database
            file : file containing the data to be ingested
            table_name: Name of the table where the data will be ingested.
        """
        self.db_connection = db_connection
        self.file = file
        self.table_name = table_name

    def create_dataframe(self):
        """
        Reads a parquet file and returns a Pandas DataFrame.

        Returns:
            pandas.DataFrame: A DataFrame with the columns converted to lowercase.
        """
        if self.file.endswith('.parquet'):
            df = pd.read_parquet(self.file)
        else:
            df = pd.read_csv(self.file)
        df.columns = df.columns.str.lower()
        return df

    def create_header(self, df):
        """
        Create a table header in the database.

        Args:
            table_name: Name of the table to be created.
            :param df: created dataframe
        """
        df.head(0).to_sql(self.table_name, con=self.db_connection, if_exists='replace')

    def ingest_chunk(self, df, chunk_size=100000):
        """
        Ingest data into the database in chunks.

        Args:
            chunk_size: Size of each chunk to be ingested (default is 100,000).
`           df: Dataframe
        Returns:
            String indicating completion of data ingestion.
            :param chunk_size:
            :param df:
        """
        df_len = len(df)
        chunk_no = round(df_len / chunk_size)

        for idx in range(0, len(df), chunk_size):
            print('Starting data ingestion into the database')
            print(f'Ingesting chunk {round((idx + chunk_size) / chunk_size)} of {chunk_no}')
            start_time = time()

            ingest_df = df[idx:idx + chunk_size]
            ingest_df.to_sql(self.table_name, con=self.db_connection, if_exists='append')

            end_time = time()
            print(f'Data ingestion chunk {round((idx + chunk_size) / chunk_size)} of {chunk_no} ended')
            print(f'Time taken: {end_time - start_time:.2f}')

        print('Ingestion completed')
