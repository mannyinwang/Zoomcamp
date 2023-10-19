from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class PostgresDB:
    """
    A class for interacting with a PostgreSQL database using SQLAlchemy.
    """

    def __init__(self, db_uri):
        """
        Constructor that initializes a SQLAlchemy engine and session.

        Parameters:
            db_uri (str): The URI for connecting to the database.
        """
        self.engine = create_engine(db_uri)
        self.Session = sessionmaker(bind=self.engine)

    def insert_data(self, table_name, data):
        """
        Insert data into a table in the database.

        Parameters:
            table_name (str): The name of the table to insert data into.
            data (dict): A dictionary of column names and their corresponding values.

        Returns:
            int: The number of rows inserted.
        """
        with self.Session() as session:
            table = session.get_bind().table(table_name)
            row = table.insert().values(**data)
            result = session.execute(row)
            session.commit()
            return result.rowcount

    def query_data(self, table_name, filter_clause=None):
        """
        Query data from a table in the database.

        Parameters:
            table_name (str): The name of the table to query data from.
            filter_clause (str): An optional SQL WHERE clause to filter the data.

        Returns:
            list: A list of dictionaries containing the queried data.
        """
        with self.Session() as session:
            table = session.get_bind().table(table_name)
            if filter_clause is None:
                rows = table.select().execute().fetchall()
            else:
                query = text(f'SELECT * FROM {table_name} WHERE {filter_clause}')
                rows = session.execute(query).fetchall()

            result = []
            for row in rows:
                result.append(dict(row))

            return result

    def delete_data(self, table_name, filter_clause):
        """
        Delete data from a table in the database.

        Parameters:
            table_name (str): The name of the table to delete data from.
            filter_clause (str): The SQL WHERE clause to filter the data to be deleted.

        Returns:
            int: The number of rows deleted.
        """
        with self.Session() as session:
            table = session.get_bind().table(table_name)
            query = table.delete().where(filter_clause)
            result = session.execute(query)
            session.commit()
            return result.rowcount
