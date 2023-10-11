# Import the EvaDB package
import evadb
from IPython.core.display import display, HTML
import os

def pretty_print(df):
    return display(HTML( df.to_html().replace("\\n","<br>")))
# Connect to EvaDB and get a database cursor for running queries
cursor = evadb.connect().cursor()
def putQueries(cursor, database_name, queries):
    outputs = []
    for query in queries:
        output.append(cursor.query(f"USE {database_name}" + " { " + f"{query}" + " }").df())
    return outputs

# Create Database
cur_database = "snowflake_data"
user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
account = os.environ["SNOWFLAKE_ACCOUNT"]
params = {
    "user": user,
    "password": password,
    "account": account,
    "database": "testdb_mg",
    "scheme": "testschema_mg"
}
drop_database_query = f"DROP DATABASE {cur_database}"
cursor.query(drop_database_query).df()
create_database_query = f"CREATE DATABASE {cur_database} WITH ENGINE = 'snowflake', PARAMETERS = {params};"
cursor.query(create_database_query).df()

# Setup Database
initial_setup_queries = [
    "CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg",
    "USE WAREHOUSE tiny_warehouse_mg",
    "CREATE DATABASE IF NOT EXISTS testdb_mg",
    "USE DATABASE testdb_mg",
    "CREATE SCHEMA IF NOT EXISTS testschema_mg",
    "USE SCHEMA testdb_mg.testschema_mg",
]
putQueries(cursor, cur_database, initial_setup_queries)

# Create Review Table
putQueries(cursor, cur_database, [
    "CREATE OR REPLACE TABLE review_table(name VARCHAR(10), review VARCHAR(1000))"
])

# Insert Reviews into SnowFlake
putQueries(cursor, cur_database, [
      "INSERT INTO review_table (name, review) VALUES ('Customer 1', 'I ordered fried rice but it is too salty.')",
      "INSERT INTO review_table (name, review) VALUES ('Customer 2', 'I ordered burger. It tastes very good and the service is exceptional.')",
      "INSERT INTO review_table (name, review) VALUES ('Customer 3', 'I ordered a takeout order, but the chicken sandwidth is missing from the order.')"
])

# Review Table Content

print(putQueries(cursor, cur_database, [
    "SELECT * FROM review_table"
]))

cursor.query("""
SELECT ChatGPT(
  "Is the review positive or negative. Only reply 'positive' or 'negative'. Here are examples. The food is very bad: negative. The food is very good: postive.",
  review
)
FROM snowflake_data.REVIEW_TABLE;
""").df()

response_df = cursor.query("""
SELECT ChatGPT(
  "Respond the the review with solution to address the review's concern",
  review
)
FROM snowflake_data.REVIEW_TABLE
WHERE ChatGPT(
  "Is the review positive or negative. Only reply 'positive' or 'negative'. Here are examples. The food is very bad: negative. The food is very good: postive.",
  review
) = "negative";
""").df()

pretty_print(response_df)