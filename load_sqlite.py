import sqlite3


class SQLiteLoader:
    def __init__(self, conn):
        try:
            self.conn = conn
            with (sqlite3.connect('db.sqlite') as conn):
                cursor = conn.cursor()
                sel_fw = "SELECT * FROM film_work"
                sel_pfw = "SELECT * FROM person_film_work"
                sel_gfw = "SELECT * FROM genre_film_work"
                self.film_work = cursor.execute(sel_fw).fetchall()
                self.genre = cursor.execute("SELECT * FROM genre").fetchall()
                self.person = cursor.execute("SELECT * FROM person").fetchall()
                self.person_film_work = cursor.execute(sel_pfw).fetchall()
                self.genre_film_work = cursor.execute(sel_gfw).fetchall()
            cursor.close()
            conn.close()
        except Exception:
            print('Не удалось собрать данные из sqlite')
            print('---------------')

    def load_movies(self, param):
        # Собирает данные таблиц из SQLITE в списки
        print('SQLITE', end='   ')
        if param == 'all':
            self.all_data = []
            self.all_data.append(self.film_work)
            self.all_data.append(self.genre)
            self.all_data.append(self.person)
            self.all_data.append(self.person_film_work)
            self.all_data.append(self.genre_film_work)
            print('База данных SQLite')
            return self.all_data
        else:
            print('Такой таблицы не существует')

    def convert_array_to_table_name(self, array):
        # Конвертирует списки даных из всех твблиц в строку 'all'
        if array == self.all_data:
            return 'all'
