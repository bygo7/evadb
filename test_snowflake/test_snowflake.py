import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine

def fetch_pandas_sqlalchemy(engine, sql):
    rows = 0
    for chunk in pd.read_sql_query(sql, engine, chunksize=50000):
        rows += chunk.shape[0]
    print(rows)

user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
account = os.environ["SNOWFLAKE_ACCOUNT"]

con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
)

con.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
con.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
con.cursor().execute("USE DATABASE testdb_mg")
con.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")

con.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
con.cursor().execute("USE DATABASE testdb_mg")
con.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")

con.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "review_table(name VARCHAR(10), review VARCHAR(1000))"
)

con.cursor().execute(
    "INSERT INTO review_table(name, review) VALUES " + 
    "    ('j', 'test string1'), " + 
    "    ('456', 'test string2')")

cur = con.cursor()
# cur.execute("SELECT table_name from information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
cur.execute("select * from review_table")
# cur.execute("select name as table_name")
for df in cur.fetch_pandas_batches():
    print(df)
    
engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database_name}/{scheme_name}'.format(
        user=user,
        password=password,
        account=account,
        database_name = 'testdb_mg',
        scheme_name = 'testschema_mg',
    )
)

try:
    connection = engine.connect()
    results = connection.execute('select * from review_table')
    print(results.fetchone())
    print(results.fetchone())
finally:
    connection.close()
    engine.dispose()
