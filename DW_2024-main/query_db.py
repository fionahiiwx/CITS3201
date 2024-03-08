import psycopg2

def connect_to_db():
    """Function to connect to the database and return a connection object."""
    try:
        connection = psycopg2.connect(
            database="AdventureworksDW",  # Update with your database name
            user="postgres",  # Update with your username
            password="1mFu0W=4dk2njy48ge.",  # Update with your password
            host="localhost",  # Update if your database is hosted elsewhere
            port="5433"  # Update with your PostgreSQL port
        )
        return connection
    except psycopg2.OperationalError as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

def query_database(connection):
    """Function to query the database and print the first row of the result."""
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dimcustomer LIMIT 10")  # Your SQL query
    result = cursor.fetchall()
    for row in result:
        print(row)
        break  # This will exit the loop after printing the first row

def main():
    connection = connect_to_db()
    if connection is not None:
        query_database(connection)
        connection.close()

if __name__ == "__main__":
    main()
