import psycopg2
from psycopg2 import OperationalError


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Connection details
db_name = "AdventureWorkDW"
db_user = "postgres"
db_password = "1mFu0W=4dk2njy48ge."  # Update with your password
db_host = "localhost"  # Update if your DB is hosted elsewhere
db_port = "5433"

# Create the connection
connection = create_connection(db_name, db_user, db_password, db_host, db_port)