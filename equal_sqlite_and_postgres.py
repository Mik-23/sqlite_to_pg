import psycopg2
from config import dsl


connection_pg = psycopg2.connect(**dsl)


def convert_datetime(array):
    # Функция для обработки значений показателей с типом дата в БД Postgres
    film_work = []
    genre = []
    person = []
    person_film_work = []
    genre_film_work = []
    final_list = []
    for data_table in array:

        # В случае, когда мы работаем с данными таблиц genre_film_work и person_film_work
        if data_table == array[-1] or data_table == array[-2]:
            for elem in data_table:
                # Переводим показатель created в строку
                created = elem[-1].strftime('%Y-%m-%d %H:%M:%S.%f+00')
                # Переводим элементы таблиц в списки
                elem_list = list(elem)
                # Удаляем значения показателя created для проставления новых
                del elem_list[-1]

                # Если милисекундные значения показателя created делятся на 100 без остатка
                if int(created[created.index('.') + 1:-3]) % 100 == 0:
                    # Делим милисекундные значения показателя created на 100 нацело
                    mili_sec = int(created[created.index('.') + 1:-3]) // 100
                    # Если полученное значение в милисекундах делится на 100 без остатка, делим на 100 нацело
                    if mili_sec % 100 == 0:
                        mili_sec //= 100
                    # Если полученное значение в милисекундах делится на 10 без остатка, делим на 10 нацело
                    elif mili_sec % 10 == 0:
                        mili_sec //= 10
                    # Добавляем к показателю created новое значение милисекунд и ставим 00 в конце
                    new_created = created[0:created.index('.') + 1] + str(mili_sec) + '+00'
                    # Добавляем в табличный список данных новые значения показателя created
                    elem_list.append(new_created)

                # Если милисекундные значения показателя created делятся на 10 без остатка
                elif int(created[created.index('.') + 1:-3]) % 10 == 0:
                    # Делим милисекундные значения показателя created на 10 нацело
                    mili_sec = int(created[created.index('.') + 1:-3]) // 10
                    # Добавляем к показателю created новое значение милисекунд и ставим 00 в конце
                    new_created = created[0:created.index('.') + 1] + str(mili_sec) + '+00'
                    # Добавляем в табличный список данных новые значения показателя created
                    elem_list.append(new_created)

                # В остальных случаях добавляем значения created в табличный список без изменений
                else:
                    elem_list.append(created)

                # Переводим списки таблиц в кортежи
                elem_tuple = tuple(elem_list)
                # Если длина кортежа (кол-во колонок в таблице) больше 4
                if len(elem_tuple) > 4:
                    # Добавляем эти кортежи в список person_film_work
                    person_film_work.append(elem_tuple)
                else:
                    # В противном случае добавляем эти кортежи в список genre_film_work
                    genre_film_work.append(elem_tuple)

        # В остальных случаях (когда мы работаем с таблицами film_work, genre, person)
        else:
            try:
                for elem in data_table:
                    # Переводим значения показателей created и modified в строки
                    created = elem[-2].strftime('%Y-%m-%d %H:%M:%S.%f+00')
                    modified = elem[-1].strftime('%Y-%m-%d %H:%M:%S.%f+00')
                    # Переводим элементы таблиц в списки
                    elem_list = list(elem)
                    # Удаляем последние 2 значения показателей (created и modified)
                    del elem_list[-2:]

                    # Если милисекундные значения показателя created делятся на 100 без остатка
                    if int(created[created.index('.') + 1:-3]) % 100 == 0:
                        # Делим милисекундные значения показателя created на 100 нацело
                        mili_sec = int(created[created.index('.') + 1:-3]) // 100
                        # Если полученное значение в милисекундах делится на 100 без остатка, делим на 100 нацело
                        if mili_sec % 100 == 0:
                            mili_sec //= 100
                        # Если полученное значение в милисекундах делится на 10 без остатка, делим на 10 нацело
                        elif mili_sec % 10 == 0:
                            mili_sec //= 10
                        # Добавляем к показателю created новое значение милисекунд и ставим 00 в конце
                        new_created = created[0:created.index('.') + 1] + str(mili_sec) + '+00'
                        # Добавляем в табличный список данных новые значения показателя created
                        elem_list.append(new_created)

                    # Если милисекундные значения показателя created делятся на 10 без остатка
                    elif int(created[created.index('.') + 1:-3]) % 10 == 0:
                        # Делим милисекундные значения показателя created на 10 нацело
                        mili_sec = int(created[created.index('.') + 1:-3]) // 10
                        # Добавляем к показателю created новое значение милисекунд и ставим 00 в конце
                        new_created = created[0:created.index('.') + 1] + str(mili_sec) + '+00'
                        # Добавляем в табличный список данных новые значения показателя created
                        elem_list.append(new_created)

                    # В остальных случаях добавляем значения created в табличный список без изменений
                    else:
                        elem_list.append(created)

                    # Если милисекундные значения показателя modified делятся на 100 без остатка
                    if int(modified[modified.index('.') + 1:-3]) % 100 == 0:
                        # Делим милисекундные значения показателя modified на 100 нацело
                        mili_sec = int(modified[modified.index('.') + 1:-3]) // 100
                        # Если полученное значение в милисекундах делится на 100 без остатка, делим на 100 нацело
                        if mili_sec % 100 == 0:
                            mili_sec //= 100
                        # Если полученное значение в милисекундах делится на 10 без остатка, делим на 10 нацело
                        elif mili_sec % 10 == 0:
                            mili_sec //= 10
                        # Добавляем к показателю modified новое значение милисекунд и ставим 00 в конце
                        new_modified = modified[0:modified.index('.') + 1] + str(mili_sec) + '+00'
                        # Добавляем в табличный список данных новые значения показателя modified
                        elem_list.append(new_modified)

                    # Если милисекундные значения показателя modified делятся на 10 без остатка
                    elif int(modified[modified.index('.') + 1:-3]) % 10 == 0:
                        # Делим милисекундные значения показателя modified на 10 нацело
                        mili_sec = int(modified[modified.index('.') + 1:-3]) // 10
                        # Добавляем к показателю modified новое значение милисекунд и ставим 00 в конце
                        new_modified = modified[0:modified.index('.') + 1] + str(mili_sec) + '+00'
                        # Добавляем в табличный список данных новые значения показателя modified
                        elem_list.append(new_modified)

                    # В остальных случаях добавляем значения modified в табличный список без изменений
                    else:
                        elem_list.append(modified)

                    # Переводим списки таблиц в кортежи
                    elem_tuple = tuple(elem_list)
                    # Если длина кортежей (кол-во колонок в таблице) больше 5
                    if len(elem_tuple) > 5:
                        # Добавляем эти кортежи в список film_work
                        film_work.append(elem_tuple)
                    # Если длина кортежей (кол-во колонок в таблице) равна 5
                    elif len(elem_tuple) == 5:
                        # Добавляем эти кортежи в список genre
                        genre.append(elem_tuple)
                    # Если длина кортежей (кол-во колонок в таблице) равна 4
                    elif len(elem_tuple) == 4:
                        # Добавляем эти кортежи в список person
                        person.append(elem_tuple)
            except TypeError:
                print('Таблица, к которой вы обращаетесь не существует в Postgres')

    # Добавляем все табличные списки в итоговый список
    final_list.append(film_work)
    final_list.append(genre)
    final_list.append(person)
    final_list.append(person_film_work)
    final_list.append(genre_film_work)
    return final_list


def update_sqlite_data(array):
    # Функция для преобразования таблиц в БД SQLite
    film_work = []
    person_film_work = []
    genre_film_work = []
    final_list = []
    for table_elem in array:

        # Если табличный элемент полного массива данных равен первому табличному элементу (таблица film_work)
        if table_elem == array[0]:
            for elem in table_elem:
                # Преобразуем элементы таблиц в списки
                list_elem = list(elem)
                # Удаляем элементы с 4 индексом (file_path) которых нет в Postgres
                del list_elem[4]
                # Преобразуем списки таблиц в кортежи
                tuple_elem = tuple(list_elem)
                # Добавляем кортежи в список film_work
                film_work.append(tuple_elem)

            """Если табличный элемент полного массива данных равен предпоследнему и последнему 
            табличным элементам (таблицы person_film_work и genre_film_work)"""

        elif table_elem == array[-1] or table_elem == array[-2]:
            for elem in table_elem:
                # Преобразуем элементы таблиц в списки
                elem_list = list(elem)
                # Находим середину таблиц (id связанных таблиц)
                center = elem_list[1:3]
                # Меняем местами id связанных таблиц для того, чтобы таблицы SQLite и Postgres соответствовали
                new_elem = tuple(elem_list[0].split() + center[::-1] + elem_list[3:])
                # Если длина кортежа (кол-во колонок в таблице) больше 4
                if len(new_elem) > 4:
                    # Добавляем кортежи в список person_film_work
                    person_film_work.append(new_elem)
                # В противном случае добавляем кортежи в список genre_film_work
                else:
                    genre_film_work.append(new_elem)
        # В остальных случаях (когда элемент соответствует таблицам genre или person) ничего не меняем
        else:
            continue

    # Добавляем табличные списки в итоговый список
    final_list.append(film_work)
    final_list.append(array[1])
    final_list.append(array[2])
    final_list.append(person_film_work)
    final_list.append(genre_film_work)
    return final_list


def equal_two_db(data_sqlite, data_postgres):
    # Функция для проверки на соответствие двух баз данных SQLite и Postgres
    flag = False
    for sqlite, postgres in zip(update_sqlite_data(data_sqlite),
                        convert_datetime(data_postgres)):
        # Если базы данных SQLite и Postgres одинаковые, то ставим flag в True
        if sorted(sqlite) == sorted(postgres):
            flag = True
    if flag:
        print('---------------')
        print('Postgresql соответствует SQLite')
    else:
        print('---------------')
        print('Postgresql не соответствует SQLite')
