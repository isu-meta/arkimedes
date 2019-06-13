import sqlite3

class DB:
    def __init__(self, addr):
        self.address = addr
        self.connection = None
        self.cursor = None

    def open_db(self):
        self.connection = sqlite3.connect(self.address)
        self.cursor = self.connection.cursor()

    def close_db(self):
        self.connection.close()
        self.connection = None
        self.cursor = None

    def _execute_edit(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self, name, fields):
        self._execute_edit(f"CREATE TABLE {name} {fields}")

    def insert_into_table(self, table, values):
        self._execute_edit(f"INSERT INTO {table} {values}")

    def update_column(self, table, field, value, condition):
        self._execute_edit(f"UPDATE {table} SET {field} = {value} WHERE {condition}")

    def delete_row(self, table, condition):
        self._execute_edit(f"DELETE FROM {table} WHERE {condition}")

