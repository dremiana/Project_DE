import csv
import psycopg2
import task_1.connection_to_postgres as ctp
import logging

# Логирование в консоль, для отслеживания ошибок
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Запрос для выборки данных
query = "SELECT * FROM dm.dm_f101_round_f"


# Функция для записи логов в таблицу log_table
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
    # Выполнение запроса
    cursor.execute(query)
    # Получение названий колонок
    column_names = [desc[0] for desc in cursor.description]
    # Получение данных
    data = cursor.fetchall()

    logging.info(f"Получено {len(data)} строк данных")
    log_event(conn, f"Получено {len(data)} строк данных")

    # Запись данных в csv
    with open('f101_data.csv', 'w+', newline='', encoding='utf-8') as csvfile:

        csvwriter = csv.writer(csvfile, delimiter=';')
        # Запись названий колонок в первую строку csv файла
        csvwriter.writerow(column_names)
        # Запись остальных данных
        csvwriter.writerows(data)

    logging.info("Данные успешно выгружены в csv файл")
    log_event(conn, "Данные успешно выгружены в csv файл")

except Exception as e:
    logging.error(f"Произошла ошибка: {str(e)}")

finally:
    if conn:
        cursor.close()
        conn.close()
        logging.info("Соединение с базой данных закрыто")
