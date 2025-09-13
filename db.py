import os
import pandas as pd
import psycopg2


class DB:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def create_table(self, table_name, columns):
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT' for col in columns])})")
            self.connection.commit()
            cursor.close()

    def insert_data(self, table_name, df):
        cursor = self.connection.cursor()
        for index, row in df.iterrows():
            values = [str(v) if v is not None else None for v in row.values]
            placeholders = ', '.join(['%s'] * len(values))
            cursor.execute(
                f'INSERT INTO {table_name} VALUES ({placeholders})', values)
        self.connection.commit()
        cursor.close()

    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results

    def select_all_data_from_table(self, table_name, limit=10):
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)

    def close(self):
        self.connection.close()
