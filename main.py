from getpass import getpass
from mysql.connector import connect, Error
import pymysql
exit_btn = ""
DbPass = input('Введите пароль от базы данных: ')
DbName = input('Введите название базы данных: ')
while (exit_btn != "1"):
    try:
        connection = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password=f"{DbPass}",
            database=f"{DbName}",
        )

        print(connection)
        print("-" * 20)
        print("Список доступных операций: Просмотреть таблицу = 1 Добавить запись = 2  Редактировать запись = 3 Удалить запись = 4")
        print("-" * 20)
        print("Аналитические запросы: Узанать среднюю стоимость услуги звукозаписи = 5 Показать наиболее используемые бренды музыкальных инструментов = 6")
        print("")
        print("Узнать, какие операционные системы чаще всего используются в студиях звукозаписи = 7 Узнать, в каком городе больше всего студий звукозаписи = 8")
        print("-" * 20)
        op_type = input("Выберите операцию: ")
        try:
            if op_type == "1":
                with connection.cursor() as cursor:
                    table_name = input("Выберите таблицу: ")
                    if table_name == "recordings":
                        cursor.execute("SELECT recordings.*, clients.*FROM recordings JOIN clients ON recordings.id_client = clients.id;")
                    elif table_name == "pcs" :
                        cursor.execute("SELECT pcs.*, sound_softwares.name AS sound_software_name, sound_equipments.brand AS sound_equipment_brand, sound_equipments.model AS sound_equipment_model FROM pcs JOIN sound_softwares ON PCs.id_sound_software = sound_softwares.id JOIN sound_equipments ON PCs.id_sound_equipment = sound_equipments.id;")
                    elif table_name == "sound_designers" :
                        cursor.execute("SELECT sd.*, c.name AS client_name, r.date AS recording_date FROM sound_designers sd JOIN clients c ON sd.id_client = c.id JOIN recordings r ON sd.id_recording = r.id;")
                    elif table_name == "studio_spaces" :
                        cursor.execute("SELECT ss.*, l.country, l.city, c.name AS client_name, r.type AS recording_type, pc.OS, pc.model AS pc_model, ac.amount AS acoustic_equipment_amount, ac.type AS acoustic_equipment_type, se.amount AS sound_equipment_amount, se.brand AS sound_equipment_brand, se.model AS sound_equipment_model, sd.name AS sound_designer_name, sd.work_time AS sound_designer_work_time FROM studio_spaces ss JOIN locations l ON ss.id_location = l.id JOIN clients c ON ss.id_client = c.id JOIN recordings r ON ss.id_recording = r.id JOIN PCs pc ON ss.id_PC = pc.id JOIN acoustic_equipments ac ON ss.id_acoustic_equipment = ac.id JOIN sound_equipments se ON ss.id_sound_equipment = se.id JOIN sound_designers sd ON ss.id_sound_designer = sd.id;")
                    else:
                        cursor.execute(f"SELECT* from `{table_name}`")
                    rows = cursor.fetchall()
                    count = 0
                    column_names = [col[0] for col in cursor.description]
                    print("-" * 100)
                    print((" "*5).join(column_names))
                    print("-" * 100)
                    for row in rows:
                        count = count + 1
                        print(count)
                        print(row)
                        print("-" * 20)
                print("Количество записей = ", count)
                exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "2":
                with connection.cursor() as cursor:
                    table_name = input("Выберите таблицу: ")
                    cursor.execute(f"SELECT* from `{table_name}`")
                    rows = cursor.fetchall()
                    column_names = [col[0] for col in cursor.description]
                    num_columns = len(column_names)
                    #print("Введите последовательно данные: ")
                    for i in range(num_columns):
                        if column_names[i] == "id":
                            column_names.append(column_names[i])
                            column_names.pop(i)
                    print((" " * 5).join(column_names))
                    add_data = []
                    for i in range(num_columns):
                        data = input(f"Введите {column_names[i]}: ")
                        add_data.append(f"\"{data}\"")
                        print(add_data[i])
                    for i in range(num_columns):
                        if column_names[i] == "id":
                            add_data.pop(i)
                    cursor.execute(f"SELECT MAX(id) INTO @max_id FROM {table_name};")
                    #cursor.execute(f"INSERT INTO {table_name}((",").join(column_names)) VALUES ((",").join(add_data),@max_id + 1);")
                    cursor.execute(f"""INSERT INTO {table_name} ({", ".join(column_names)}) VALUES ({", ".join(add_data)}, @max_id + 1);""")
                    connection.commit()
                    exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "3" :
                with connection.cursor() as cursor:
                    record_id = ""
                    up_data1 = "id"
                    up_data2 = ""
                    cycle_exit = ""
                    table_name = input("Выберите таблицу: ")
                    cursor.execute(f"SELECT* from `{table_name}`")
                    rows = cursor.fetchall()
                    count = 0
                    column_names = [col[0] for col in cursor.description]
                    print("-" * 100)
                    print((" " * 5).join(column_names))
                    print("-" * 100)
                    record_id = input("Выберите запись для редактирования: id = ")
                    cursor.execute(f"SELECT* FROM {table_name} WHERE id = {record_id};")
                    while (cycle_exit != "1") :
                        while (up_data1 == "id"):
                            up_data1 = input("Введите название данных, которые хотите изменить: ")
                            if (up_data1 == "id") :
                                print("id записи изменять не рекомендуется")
                        up_data2 = input("Измененные данные: ")
                        try:
                            cursor.execute(f"UPDATE {table_name} SET {up_data1} = '{up_data2}' WHERE id = {record_id}")
                        except:
                            print(f"Ошибка, связанная с нарушением ограничей внешнего ключа. Прежде чем выполнить операцию обновления в таблице {table_name}, вам необходимо сначала удалить или обновить связанные дочерние записи")
                        connection.commit()
                        cycle_exit = input(f"Для выхода из редактирования таблицы {table_name} нажмите 1, для редактирования других данных нажмите 2: ")
                    exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "4" :
                with connection.cursor() as cursor:
                    table_name = input("Выберите таблицу: ")
                    cursor.execute(f"SELECT* from `{table_name}`")
                    rows = cursor.fetchall()
                    count = 0
                    column_names = [col[0] for col in cursor.description]
                    print("-" * 100)
                    print((" " * 5).join(column_names))
                    print("-" * 100)
                    record_id = ""
                    del_aprove = ""
                    record_id = input("Выберите запись для удаления: id = ")
                    cursor.execute(f"SELECT* FROM {table_name} WHERE id = {record_id};")
                    del_aprove = input("Вы действительно хотите удалить данную запись? 1 - ДА 2 - НЕТ ")
                    if del_aprove == "1" :
                        if table_name == 'sound_designers':
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_sound_designer = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_sound_designer = {record_id};")
                            cursor.execute(f"DELETE FROM sound_designers WHERE id = {record_id};")
                        elif table_name == 'acousitc_equipments':
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_acoustic_equipment = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_acoustic_equipment = {record_id};")
                            cursor.execute(f"DELETE FROM acoustic_equipment WHERE id = {record_id};")
                        elif table_name == 'clients'  :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_client = {record_id});")
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_client = {record_id}));")
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_recording IN (SELECT id FROM recordings WHERE id_client = {record_id})));")
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_recording IN (SELECT id FROM recordings WHERE id_client = {record_id}));")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_client = {record_id};")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_client = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_recording IN (SELECT id FROM recordings WHERE id_client = {record_id}));")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_recording IN (SELECT id FROM recordings WHERE id_client = {record_id});")
                            cursor.execute(f"DELETE FROM sound_designers WHERE id_client = {record_id};")
                            cursor.execute(f"DELETE FROM sound_designers WHERE id_recording IN (SELECT id FROM recordings WHERE id_client = {record_id});")
                            cursor.execute(f"DELETE FROM recordings WHERE id_client = {record_id};")
                            cursor.execute(f"DELETE FROM clients WHERE id = {record_id}")
                        elif table_name == 'locations' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_location = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_location = {record_id};")
                            cursor.execute(f"DELETE FROM locations WHERE id = {record_id};")
                        elif table_name == 'musical_instruments' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_musical_instrument = {record_id});")
                            cursor.execute(f"DELETE FROM musical_instruments WHERE id = {record_id};")
                        elif table_name == 'pcs' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_PC = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_PC = {record_id};")
                            cursor.execute(f"DELETE FROM pcs WHERE id = {record_id};")
                        elif table_name == 'recordings' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_recording = {record_id});")
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_recording = {record_id}));")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_recording = {record_id};")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_sound_designer IN (SELECT id FROM sound_designers WHERE id_recording = {record_id});")
                            cursor.execute(f"DELETE FROM sound_designers WHERE id_recording = {record_id};")
                            cursor.execute(f"DELETE FROM recordings WHERE id = {record_id};")
                        elif table_name == 'sound_equipments' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_sound_equipment = {record_id});")
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_PC IN (SELECT id FROM pcs WHERE id_sound_equipment = {record_id}));")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_PC IN (SELECT id FROM pcs WHERE id_sound_equipment = {record_id});")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_sound_equipment = {record_id};")
                            cursor.execute(f"DELETE FROM pcs WHERE id_sound_equipment = {record_id};")
                            cursor.execute(f"DELETE FROM sound_equipments WHERE id = {record_id};")
                        elif table_name == 'sound_softwares' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space IN (SELECT id FROM studio_spaces WHERE id_PC IN (SELECT id FROM pcs WHERE id_sound_software = {record_id}));")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id_PC IN (SELECT id FROM pcs WHERE id_sound_software = {record_id});")
                            cursor.execute(f"DELETE FROM pcs WHERE id_sound_software = {record_id};")
                            cursor.execute(f"DELETE FROM sound_softwares WHERE id = {record_id};")
                        elif table_name == 'studio_spaces' :
                            cursor.execute(f"DELETE FROM musical_instruments_in_studios WHERE id_studio_space = {record_id};")
                            cursor.execute(f"DELETE FROM studio_spaces WHERE id = {record_id};")
                        connection.commit()
                    exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "5" :
                with connection.cursor() as cursor:
                    cursor.execute("SELECT AVG(price) AS avg_recording_cost FROM recordings;")
                    rows = cursor.fetchall()
                    for row in rows:
                        print(row)
                        print("-" * 20)
                exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "6" :
                with connection.cursor() as cursor:
                    count = 0
                    cursor.execute("SELECT brand, COUNT(*) AS usage_count FROM musical_instruments JOIN musical_instruments_in_studios ON musical_instruments.id = musical_instruments_in_studios.id_musical_instrument GROUP BY brand ORDER BY usage_count DESC LIMIT 5;")
                    rows = cursor.fetchall()
                    for row in rows:
                        count = count + 1
                        print(count)
                        print(row)
                        print("-" * 20)
                exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "7" :
                with connection.cursor() as cursor:
                    count = 0
                    cursor.execute("SELECT os, COUNT(*) AS usage_count FROM (SELECT CASE WHEN pcs.os LIKE '%Windows%' THEN 'Windows' WHEN pcs.os LIKE '%macOS%' THEN 'macOS' ELSE 'Other' END AS os FROM PCs ) AS os_counts GROUP BY os ORDER BY usage_count DESC;")
                    rows = cursor.fetchall()
                    for row in rows:
                        count = count + 1
                        print(count)
                        print(row)
                        print("-" * 20)
                exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")
            elif op_type == "8" :
                with connection.cursor() as cursor:
                    count = 0
                    cursor.execute("SELECT city, COUNT(*) AS num_studios FROM locations JOIN studio_spaces ON locations.id = studio_spaces.id_location GROUP BY city ORDER BY num_studios DESC LIMIT 1;")
                    rows = cursor.fetchall()
                    for row in rows:
                        print(row)
                        print("-" * 20)
                exit_btn = input("Чтобы выйти нажмите 1, Чтобы выбрать операцию нажмите 2: ")


















        finally:
            connection.close()
    except Error as e:
        print(e)