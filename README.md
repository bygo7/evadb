<p align="center">
  <a href="https://evadb.readthedocs.io">
    <img src="https://raw.githubusercontent.com/georgia-tech-db/evadb/master/docs/images/evadb/evadb-full-logo.svg" width="40%" alt="EvaDB">
  </a>
</p>

<p align="center"><i><b>CS6422 Coursework Project</b></i></p>
<p align="center"><i><b>Integration of SnowFlake with  <a href="https://github.com/georgia-tech-db/evadb">EvaDB</a>, Database system for AI-powered apps</b></i></p>


## Goal: Utilize Snowflake with EvaDB

### Key features to implement:
1.	Support ‘CREATE DATABASE WITH ENGINE  “snowflake”’
2.	Support SQL APIs including but not limited to ‘SELECT’, ‘FROM’, ‘LIMIT’, ‘DESCRIBE’, ‘JOIN’, etc.
3.	Support workflow automation using ‘CREATE JOB’

### Key components to implement the key features:
1.	The middle layer connects EvaDB with the Snowflake, which should be able to
a.	Handle authorization works
b.	Translate SQL queries on EvaDB to Snowflake APIs
c.	Send API calls to the Snowflake database and get returned data

### Ways to implement:
1.	Add “snowflake” and related functions to the existing EvaDB codebase along with the currently supported PostgreSQL, SQLite, MySQL, and MariaDB.
2.	Write parsers and script functions to translate SQL query on EvaDB to snowflake API calls

### Implementation details:
1.	Modify the parser on evadb/parser to accept SnowFlake as a valid third-party database schema and make parsing functions to pass queries to the handler
2.	Write SnowflakeHandler on third_party/databases/snowflake to handle queries related to SnowFlake
    a.	<b>Connect()</b>: utilizes snowflake.connector module to connect SnowFlake with given parameters(id, password, etc.). The original implementation here was to connect the database with a new connector for every query, but this time snowflake connector is implemented as a static object inside the handler to support the non-context switch after ‘USE DATABASE’ and ‘USE SCHEME’ queries. (The original method didn’t support new queries in the ‘DATABASE’ context after ‘USE DATABASE’ query because of new initialization)
    b.	<b>_pg_to_python_types()</b>: added SnowFlake-supported data types here
    c.	<b>Get_tables() and get_columns()</b>: changed functions to utilize self.execute_native_query to get desired data
    d.	<b>get_sqlalchmey_uri()</b>: added snowflake-type sqlalchemey uri here

## Tutorial - Review Analysis and Respond with SnowFlake and ChatGPT
Github: https://github.com/bygo7/evadb-snowflake/tree/staging/test_snowflake

Queries:
```
"CREATE OR REPLACE TABLE review_table(name VARCHAR(10), review VARCHAR(1000))"
 "INSERT INTO review_table (name, review) VALUES ('Customer 1', 'I ordered fried rice but it is too salty.')",
 "INSERT INTO review_table (name, review) VALUES ('Customer 2', 'I ordered burger. It tastes very good and the service is exceptional.')",
"INSERT INTO review_table (name, review) VALUES ('Customer 3', 'I ordered a takeout order, but the chicken sandwidth is missing from the order.')"
"SELECT * FROM review_table"
```
```
"""
SELECT ChatGPT(
  "Is the review positive or negative. Only reply 'positive' or 'negative'. Here are examples. The food is very bad: negative. The food is very good: postive.",
  review
)
FROM snowflake_data.REVIEW_TABLE;
"""
```
```
"""
SELECT ChatGPT(
  "Respond the the review with solution to address the review's concern",
  review
)
FROM snowflake_data.REVIEW_TABLE
WHERE ChatGPT(
  "Is the review positive or negative. Only reply 'positive' or 'negative'. Here are examples. The food is very bad: negative. The food is very good: postive.",
  review
) = "negative";
"""
```
### Output:
```
From "SELECT * FROM review_table"
[         name                                             review
0  Customer 1          I ordered fried rice but it is too salty.
1  Customer 2  I ordered burger. It tastes very good and the ...
2  Customer 3  I ordered a takeout order, but the chicken san...]
```

From the first ChatGPT query
      <img width="90%" src="https://github.com/bygo7/evadb-snowflake/tree/staging/data/assets/snowflake_query1" alt="EvaDB Star History Chart">


From the second ChatGPT query
      <img width="90%" src="https://github.com/bygo7/evadb-snowflake/tree/staging/data/assets/snowflake_query2" alt="EvaDB Star History Chart">


### Lessons learned
1.	EvaDB’s scalable structure through adding SnowFlake as another third-party database model
2.	Learned about APIs for databases, especially SnowFlake, and APIs for LLMs, especially OpenAI

### References
https://evadb.readthedocs.io/en/stable/
https://colab.research.google.com/github/georgia-tech-db/eva/blob/staging/tutorials/14-food-review-tone-analysis-and-response.ipynb#scrollTo=QCX7gDJuiV15
https://docs.snowflake.com/en/developer-guide/sql-api/index
https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy
