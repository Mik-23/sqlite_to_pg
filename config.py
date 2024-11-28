import sqlite3


dsl = {'dbname': 'movies_database',
       'user': 'postgres',
       'password': '123qwe',
       'host': 'localhost',
       'port': 5432}
connection_sqlite = sqlite3.connect('db.sqlite')
