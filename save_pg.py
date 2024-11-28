import psycopg2
from config import dsl
from load_sqlite import SQLiteLoader


class PostgresSaver(SQLiteLoader):
    def __init__(self, con_pg):
        self.con_pg = con_pg

    def create_cursor(self):
        # Создаёт курсор
        with psycopg2.connect(**dsl) as self.con_pg:
            cur = self.con_pg.cursor()
        return cur

    def get_table(self, table):
        # Получает данных из таблиц в Postgres
        cur = self.create_cursor()
        if table == 'film_work':
            cur.execute("SELECT * FROM content.film_work")
            film_work = cur.fetchall()
            return film_work
        elif table == 'genre':
            cur.execute("SELECT * FROM content.genre")
            genre = cur.fetchall()
            return genre
        elif table == 'person':
            cur.execute("SELECT * FROM content.person")
            person = cur.fetchall()
            return person
        elif table == 'person_film_work':
            cur.execute("SELECT * FROM content.person_film_work")
            person_film_work = cur.fetchall()
            return person_film_work
        elif table == 'genre_film_work':
            cur.execute("SELECT * FROM content.genre_film_work")
            genre_film_work = cur.fetchall()
            return genre_film_work

    def save_film_work(self, data):
        # Загружает фильмы в Postgres
        film_work = []
        # Создаём список из id таблицы film_work БД Postgres
        film_work_ids = list(map(lambda x:x[0], self.get_table('film_work')))
        for elem in data[0]:
            # Переводим элементы таблицы с фильмами в списки
            list_elem = list(elem)
            # Удаляем элементы с 4 индексом (file_path) которых нет в Postgres
            del list_elem[4]

            """Если id элементов в таблице film_work из БД SQLite нет в той же таблице, 
            но в БД Postgres, то нужно их загрузить в n-ом кол-ве"""

            if list_elem[0] not in film_work_ids:

                """Меняем время в часах у показателей created и
                modified с 3 до 20, чтобы эти показатели в Postgres
                соответствовали этим показателям в SQLite.
                Для этого создадим левую и правую границы, прибавим 17 и
                и удалим старые значения показателей"""

                creat_left = list_elem[-2][0:list_elem[-2].index(' ')]
                creat_right = list_elem[-2][list_elem[-2].index(':'):-1]
                new_creat = creat_left + ' 17' + creat_right
                mod_left = list_elem[-1][0:list_elem[-1].index(' ')]
                mod_right = list_elem[-1][list_elem[-1].index(':'):-1]
                new_mod = mod_left + ' 17' + mod_right
                # Удаляем последние 2 значения показателей (created и modified)
                del list_elem[-2:]
                # Добавим новые показатели с датой  в список
                list_elem.append(new_creat)
                list_elem.append(new_mod)
                # Переводим списки в кортежи
                tuple_elem = tuple(list_elem)
                # Добавляем кортежи в итоговый список с фильмами
                film_work.append(tuple_elem)
        try:
            cur = self.create_cursor()
            query = """INSERT INTO content.film_work
            (id, title, description, creation_date, rating, type, created, modified)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"""
            # Добавляем список фильмов в БД Postgres
            postgres_film_work = (cur.executemany(query, film_work))
            self.con_pg.commit()
            print(f"Фильмы записались в БД postgres в кол-ве {len(film_work)} штук")
            return postgres_film_work
        except Exception:
            print(f'Таблица с фильмами уже заполнена')
        finally:
            cur.close()
            self.con_pg.close()

    def save_genre(self, data):
        # Загружает жанры в Postgres
        genre = []
        # Создаём список из id таблицы genre БД Postgres
        genre_ids = list(map(lambda x:x[0], self.get_table('genre')))
        for elem in data[1]:

            """Если id элементов в таблице genre из БД SQLite нет в той же таблице, 
            но в БД Postgres, то нужно их загрузить в n-ом кол-ве"""

            if elem[0] not in genre_ids:
                # Переводим элементы таблицы с жанрами в списки
                list_elem = list(elem)

                """Меняем время в часах у показателей created и
                modified с 3 до 20, чтобы эти показатели в Postgres
                соответствовали этим показателям в SQLite.
                Для этого создадим левую и правую границы, прибавим 17 и
                и удалим старые значения показателей"""

                creat_left = list_elem[-2][0:list_elem[-2].index(' ')]
                creat_right = list_elem[-2][list_elem[-2].index(':'):-1]
                new_creat = creat_left + ' 17' + creat_right
                mod_left = list_elem[-1][0:list_elem[-1].index(' ')]
                mod_right = list_elem[-1][list_elem[-1].index(':'):-1]
                new_mod = mod_left + ' 17' + mod_right
                # Удаляем последние 2 значения показателей (created и modified)
                del list_elem[-2:]
                # Добавим новые показатели с датой  в список
                list_elem.append(new_creat)
                list_elem.append(new_mod)
                # Переводим списки в кортежи
                tuple_elem = tuple(list_elem)
                # Добавляем кортежи в итоговый список с жанрами
                genre.append(tuple_elem)
        try:
            cur = self.create_cursor()
            query = """INSERT INTO content.genre
            (id, name, description, created, modified) 
            VALUES(%s, %s, %s, %s, %s)"""
            # Добавляем список жанров в БД Postgres
            postgres_genre = (cur.executemany(query, genre))
            self.con_pg.commit()
            print(f"Жанры записались в БД postgres в кол-ве {len(genre)} штук")
            return postgres_genre
        except Exception:
            print(f'Таблица с жанрами уже заполнена')
        finally:
            cur.close()
            self.con_pg.close()

    def save_person(self, data):
        # Загружает участников в Postgres
        person = []
        # Создаём список из id таблицы person БД Postgres
        person_ids = list(map(lambda x: x[0], self.get_table('person')))
        for elem in data[2]:

            """Если id элементов в таблице person из БД SQLite нет в той же таблице, 
            но в БД Postgres, то нужно их загрузить в n-ом кол-ве"""

            if elem[0] not in person_ids:
                # Переводим элементы таблицы с участниками в списки
                list_elem = list(elem)

                """Меняем время в часах у показателей created и
                modified с 3 до 20, чтобы эти показатели в Postgres
                соответствовали этим показателям в SQLite.
                Для этого создадим левую и правую границы, прибавим 17 и
                и удалим старые значения показателей"""

                creat_left = list_elem[-2][0:list_elem[-2].index(' ')]
                creat_right = list_elem[-2][list_elem[-2].index(':'):-1]
                new_creat = creat_left + ' 17' + creat_right
                mod_left = list_elem[-1][0:list_elem[-1].index(' ')]
                mod_right = list_elem[-1][list_elem[-1].index(':'):-1]
                new_mod = mod_left + ' 17' + mod_right
                # Удаляем последние 2 значения показателей (created и modified)
                del list_elem[-2:]
                # Добавим новые показатели с датой  в список
                list_elem.append(new_creat)
                list_elem.append(new_mod)
                # Переводим списки в кортежи
                tuple_elem = tuple(list_elem)
                # Добавляем кортежи в итоговый список с участниками
                person.append(tuple_elem)
        try:
            cur = self.create_cursor()
            query = """INSERT INTO content.person
            (id, full_name, created, modified)
            VALUES(%s, %s, %s, %s)"""
            # Добавляем список участников в БД Postgres
            postgres_person = (cur.executemany(query, person))
            print(f"Участники записались в БД postgres в кол-ве {len(person)} штук")
            self.con_pg.commit()
            return postgres_person
        except Exception:
            print('Таблица с участниками уже заполнена')
        finally:
            cur.close()
            self.con_pg.close()

    def save_person_film_work(self, data):
        # Загружает участников фильмов в Postgres
        person_film_work = []
        # Создаём список из id таблицы person_film_work БД Postgres
        person_film_work_ids = list(map(lambda x: x[0], self.get_table('person_film_work')))
        for elem in data[3]:
            # Переводим элементы таблицы с участниками фильмов в списки
            list_elem = list(elem)

            """Если id элементов в таблице person_film_work из БД SQLite нет в той же таблице, 
            но в БД Postgres, то нужно их загрузить в n-ом кол-ве"""

            if list_elem[0] not in person_film_work_ids:
                new_list_elem = []

                """Добавим значения показателей в новый список
                 в соответсвие с порядком следовании колонок таблиц в Postgres"""

                new_list_elem.append(list_elem[0])
                new_list_elem.append(list_elem[2])
                new_list_elem.append(list_elem[1])
                new_list_elem.append(list_elem[3])
                new_list_elem.append(list_elem[4])

                """Меняем время в часах у показателя created
                с 3 до 20, чтобы этот показатель в Postgres
                соответствовал этому показателю в SQLite.
                Для этого создадим левую и правую границы, прибавим 17 и
                и удалим старые значения показателя"""

                creat_left = list_elem[-1][0:list_elem[-1].index(' ')]
                creat_right = list_elem[-1][list_elem[-1].index(':'):-1]
                new_creat = creat_left + ' 17' + creat_right
                del new_list_elem[-1]
                # Добавим новый показатель с датой в список
                new_list_elem.append(new_creat)
                # Переводим списки в кортежи
                tuple_elem = tuple(new_list_elem)
                # Добавляем кортежи в итоговый список с участниками фильмов
                person_film_work.append(tuple_elem)

        try:
            cur = self.create_cursor()
            query = """INSERT INTO content.person_film_work
            (id, person_id, film_work_id, role, created)
            VALUES(%s, %s, %s, %s, %s)"""
            # Добавляем список с участниками фильмов в БД Postgres
            postgres_person_film_work = (cur.executemany(query, person_film_work))
            print(f"Участники фильмов записались в БД postgres в кол-ве {len(person_film_work)} штук")
            self.con_pg.commit()
            return postgres_person_film_work
        except Exception as e:
            print(f'Таблица с участниками фильмов уже заполнена {e}')
        finally:
            cur.close()
            self.con_pg.close()

    def save_genre_film_work(self, data):
        # Загружает жанры фильмов в Postgres
        genre_film_work = []
        # Создаём список из id таблицы genre_film_work БД Postgres
        genre_film_work_ids = list(map(lambda x: x[0], self.get_table('genre_film_work')))
        for elem in data[4]:
            # Переводим элементы таблицы с жанрами фильмов в списки
            list_elem = list(elem)

            """Если id элементов в таблице genre_film_work из БД SQLite нет в той же таблице, 
            но в БД Postgres, то нужно их загрузить в n-ом кол-ве"""

            if list_elem[0] not in genre_film_work_ids:
                new_list_elem = []

                """Добавим значения показателей в новый список
                в соответсвие с порядком следовании колонок таблиц в Postgres"""

                new_list_elem.append(list_elem[0])
                new_list_elem.append(list_elem[2])
                new_list_elem.append(list_elem[1])
                new_list_elem.append(list_elem[3])

                """Меняем время в часах у показателя created
                с 3 до 20, чтобы этот показатель в Postgres
                соответствовал этому показателю в SQLite.
                Для этого создадим левую и правую границы, прибавим 17 и
                и удалим старые значения показателя"""

                creat_left = list_elem[-1][0:list_elem[-1].index(' ')]
                creat_right = list_elem[-1][list_elem[-1].index(':'):-1]
                new_creat = creat_left + ' 17' + creat_right
                del new_list_elem[-1]
                # Добавим новый показатель с датой в список
                new_list_elem.append(new_creat)
                # Переводим списки в кортежи
                tuple_elem = tuple(new_list_elem)
                # Добавляем кортежи в итоговый список с участниками фильмов
                genre_film_work.append(tuple_elem)
        try:
            cur = self.create_cursor()
            query = """INSERT INTO content.genre_film_work
            (id, genre_id, film_work_id, created)
            VALUES(%s, %s, %s, %s)"""
            # Добавляем список с жанрами фильмов в БД Postgres
            postgres_genre_film_work = (cur.executemany(query, genre_film_work))
            print(f"Жанры фильмов записались в БД postgres в кол-ве {len(genre_film_work)} штук")
            self.con_pg.commit()
            return postgres_genre_film_work
        except Exception:
            print('Таблица с жанрами фильмов уже заполнена')
        finally:
            cur.close()
            self.con_pg.close()

    def save_all_data(self, data):
        # Собирает данные всех таблиц из БД Postgres в один список
        all_data = []
        film_work = self.save_film_work(data)
        genre = self.save_genre(data)
        person = self.save_person(data)
        person_film_work = self.save_person_film_work(data)
        genre_film_work = self.save_genre_film_work(data)
        all_data.append(film_work)
        all_data.append(genre)
        all_data.append(person)
        all_data.append(person_film_work)
        all_data.append(genre_film_work)
        return all_data

    def load_movies(self, param):
        # Получает данные из Postgres
        print('---------------')
        print('POSTGRES', end='   ')
        if param == 'all_pg':
            all_data = []
            all_data.append(self.get_table('film_work'))
            all_data.append(self.get_table('genre'))
            all_data.append(self.get_table('person'))
            all_data.append(self.get_table('person_film_work'))
            all_data.append(self.get_table('genre_film_work'))
            print('База данных Postgres')
            return all_data
        else:
            print('Такой таблицы не существует')
