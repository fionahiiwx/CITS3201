import pandas as pd
from sqlalchemy import create_engine

connection_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create the engine
engine = create_engine(connection_url)

df = pd.read_sql_query("SELECT * FROM dimcustomer LIMIT 10", engine)