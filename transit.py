import sqlite3
import psycopg
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row
from load_sqlite import SQLiteLoader
from save_pg import PostgresSaver
from equal_sqlite_and_postgres import equal_two_db
from config import dsl


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection, table_name):
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_loader = SQLiteLoader(connection)
    postgres_saver = PostgresSaver(pg_conn)
    # Создаём массивы данных для передачи
    data_sqlite = sqlite_loader.load_movies(table_name)
    data_postgres = postgres_saver.load_movies('all_pg')
    # Передаём все данные из базы данных SQLite в Postgres
    postgres_saver.save_all_data(data_sqlite)
    equal_two_db(data_sqlite, data_postgres)


if __name__ == '__main__':
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn, 'all')
