import csv
import psycopg2
import task_1.connection_to_postgres as ctp
import logging

# Логирование в консоль, для отслеживания ошибок
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

csv_file = 'f101_data.csv'


def log_event(conn, event):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO LOGS.log_table (log_data, log_time) VALUES (%s, LOCALTIMESTAMP(0));", (event,))
        conn.commit()


try:
    # Подключение к базе данных
    conn = psycopg2.connect(host=ctp.host, database=ctp.dbname, user=ctp.user, password=ctp.password)
    cursor = conn.cursor()

    logging.info("Подключение к базе данных установлено")
    log_event(conn, "Подключение к базе данных установлено")

    # Создание копии таблицы dm.dm_f101_round_f
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2
    (LIKE dm.dm_f101_round_f INCLUDING ALL)
    """)
    conn.commit()

    logging.info("Создана копия таблицы dm.dm_f101_round_f_v2")
    log_event(conn, "Создана копия таблицы dm.dm_f101_round_f_v2")

    # Чтение данных из csv
    with open(csv_file, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=';')
        headers = next(csvreader)

        # Запрос для вставки данных в таблицу
        insert_query = f"""
        INSERT INTO dm.dm_f101_round_f_v2 ({','.join(headers)})
        VALUES ({','.join(['%s' for _ in headers])})
        """

        # Вставляем данные в таблицу
        for row in csvreader:
            # Заменяем пустые строки на None
            row = [None if value == '' else value for value in row]
            cursor.execute(insert_query, row)

        conn.commit()

    logging.info("Данные в таблицу dm.dm_f101_round_f_v2 добавлены")
    log_event(conn, "Данные в таблицу dm.dm_f101_round_f_v2 добавлены")

except Exception as e:
    logging.error(f"Произошла ошибка: {str(e)}")

finally:
    if conn:
        cursor.close()
        conn.close()
        logging.info("Соединение с базой данных закрыто")
