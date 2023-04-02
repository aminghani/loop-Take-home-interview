import pandas as pd
import psycopg2
from . import database


def read_sql_data(table):
    """
    Executes the specified SQL query on the Postgres database using the provided connection details.
    Returns a Pandas dataframe containing the query results.
    """

    query = f"SELECT * FROM {table}"

    # Establish a database connection
    cnx = psycopg2.connect(**database.db_config)

    # Execute the query and read the results into a Pandas dataframe
    df = pd.read_sql(query, con=cnx)

    # Close the database connection
    cnx.close()

    # Return the dataframe
    return df
