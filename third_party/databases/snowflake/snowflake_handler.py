import snowflake.connector
import pandas as pd
import numpy as np

from evadb.third_party.databases.types import (
    DBHandler,
    DBHandlerResponse,
    DBHandlerStatus,
)


class SnowFlakeHandler(DBHandler):
    connection = None
    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.account = kwargs.get("account")
        self.database = kwargs.get("database")
        self.scheme = kwargs.get("scheme")
        # print("SnowFlake Handler Initialization...")
        
    def connect(self):
        if SnowFlakeHandler.connection is None:
            try:
                SnowFlakeHandler.connection = snowflake.connector.connect(
                    user=self.user,
                    password=self.password,
                    account=self.account,
                )
                # print(self.user, self.password, self.account)
                return DBHandlerStatus(status=True)
            except snowflake.connector.Error as e:
                return DBHandlerStatus(status=False, error=str(e))
        else:
            return DBHandlerStatus(status=True)
    
    def disconnect(self):
        pass
        # if SnowFlakeHandler.connection:
            # SnowFlakeHandler.connection.close()
    
    def check_connection(self) -> DBHandlerStatus:
        if SnowFlakeHandler.connection:
            return DBHandlerStatus(status=True)
        else:
            return DBHandlerStatus(status=False, error="Not connected to the database.")
    
    def get_tables(self) -> DBHandlerResponse:
        if not SnowFlakeHandler.connection:
            return DBHandlerResponse(data=None, error="Not connected to the database.")
        try:
            query = "SELECT table_name from information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
            tables_df = self.execute_native_query(query).data
            return DBHandlerResponse(data=tables_df)
        except snowflake.connector.Error as e:
            # print("error excep", str(e))
            return DBHandlerResponse(data=None, error=str(e))

    def get_columns(self, table_name: str) -> DBHandlerResponse:
        """
        Returns the list of columns for the given table.
        Args:
            table_name (str): name of the table whose columns are to be retrieved.
        Returns:
            DBHandlerResponse
        """
        if not SnowFlakeHandler.connection:
            return DBHandlerResponse(data=None, error="Not connected to the database.")

        try:
            # print("get_columns::")
            query = f"SELECT column_name as name, data_type as dtype, udt_name FROM information_schema.columns WHERE table_name='{table_name}'"

            columns_df = self.execute_native_query(query).data
            # print(columns_df)
            columns_df["dtype"] = columns_df.apply(
                lambda x: self._pg_to_python_types(x["dtype"], x["udt_name"]), axis=1
            )
            return DBHandlerResponse(data=columns_df)
        except snowflake.connector.Error as e:
            # print("except", e)
            return DBHandlerResponse(data=None, error=str(e))
        
    def _fetch_results_as_df(self, cursor):
        """
        This is currently the only clean solution that we have found so far.
        Reference to MySQL API: https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-fetchall.html
        In short, currently there is no very clean programming way to differentiate
        CREATE, INSERT, SELECT. CREATE and INSERT do not return any result, so calling
        fetchall() on those will yield a programming error. Cursor has an attribute
        rowcount, but it indicates # of rows that are affected. In that case, for both
        INSERT and SELECT rowcount is not 0, so we also cannot use this API to
        differentiate INSERT and SELECT.
        """
        try:
            res = cursor.fetchall()
            if not res:
                return pd.DataFrame({"status": ["success"]})
            res_df = pd.DataFrame(
                res, columns=[desc[0].lower() for desc in cursor.description]
            )
            return res_df
        except snowflake.connector.ProgrammingError as e:
            if str(e) == "no results to fetch":
                return pd.DataFrame({"status": ["success"]})
            raise e
    
    def execute_native_query(self, query_string: str) -> DBHandlerResponse:
        # print(SnowFlakeHandler.connection)
        # print("snowflake native executor::", query_string)
        if not SnowFlakeHandler.connection:
            return DBHandlerResponse(data=None, error="Not connected to the database.")

        try:
            cursor = SnowFlakeHandler.connection.cursor()
            # print(cursor)
            cursor.execute(query_string)
            return DBHandlerResponse(data=self._fetch_results_as_df(cursor))
        except snowflake.connector.Error as e:
            return DBHandlerResponse(data=None, error=str(e))
    
    def get_sqlalchmey_uri(self) -> str:
        return f"snowflake://{self.user}:{self.password}@{self.account}/{self.database}/{self.scheme}"
    
    def _pg_to_python_types(self, pg_type: str, udt_name: str):
        primitive_type_mapping = {
            "integer": int,
            "bigint": int,
            "smallint": int,
            "numeric": float,
            "real": float,
            "double precision": float,
            "character": str,
            "character varying": str,
            "TEXT": str,
            "boolean": bool,
            # Add more mappings as needed
        }

        user_defined_type_mapping = {
            "vector": np.ndarray
            # Handle user defined types constructed by Postgres extension.
        }

        if pg_type in primitive_type_mapping:
            return primitive_type_mapping[pg_type]
        elif pg_type == "USER-DEFINED" and udt_name in user_defined_type_mapping:
            return user_defined_type_mapping[udt_name]
        else:
            raise Exception(
                f"Unsupported column {pg_type} encountered in the postgres table. Please raise a feature request!"
            )