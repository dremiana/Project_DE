import psycopg2
import time
import task_1.tables_pd as tables
import connection_to_postgres as ctp
from contextlib import contextmanager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@contextmanager
def get_connection():
    conn = psycopg2.connect(host=ctp.host, database=ctp.dbname, user=ctp.user, password=ctp.password)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(conn):
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def sql_query(conn, query, parameters=None):
    with get_cursor(conn) as cur:
        if parameters is None:
            cur.execute(query)
        else:
            cur.execute(query, parameters)
        conn.commit()


def log_event(conn, event):
    sql_query(conn, "INSERT INTO LOGS.log_table (log_data, log_time) VALUES (%s, LOCALTIMESTAMP(0));", (event,))


def data_loading(conn):
    def insert_data(table_name, rows, query):
        log_event(conn, f"Начало загрузки данных в таблицу {table_name}")
        for row in rows.itertuples(index=False, name=None):
            try:
                sql_query(conn, query, row)
            except Exception as e:
                logging.error(f"Ошибка при загрузке данных в таблицу {table_name}: {row}")
                logging.error(e)
                raise e
        time.sleep(5)  # Simulate time-consuming task
        log_event(conn, f"Данные в таблицу {table_name} добавлены")
        logging.info(f"Данные в таблицу {table_name} добавлены")

    # Обновление запросов, чтобы количество параметров совпадало с количеством колонок в таблице

    insert_data('FT_BALANCE_F', tables.balance,
                "INSERT INTO DS.FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out) VALUES (%s, %s, %s, %s) ON CONFLICT (on_date, account_rk) DO UPDATE SET currency_rk = EXCLUDED.currency_rk, balance_out = EXCLUDED.balance_out;")

    insert_data('MD_ACCOUNT_D', tables.account,
                "INSERT INTO DS.MD_ACCOUNT_D (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (data_actual_date, account_rk) DO UPDATE SET data_actual_end_date = EXCLUDED.data_actual_end_date, account_number = EXCLUDED.account_number, char_type = EXCLUDED.char_type, currency_rk = EXCLUDED.currency_rk, currency_code = EXCLUDED.currency_code;")

    insert_data('MD_CURRENCY_D', tables.currency,
                "INSERT INTO DS.MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (currency_rk, data_actual_date) DO UPDATE SET data_actual_end_date = EXCLUDED.data_actual_end_date, currency_code = EXCLUDED.currency_code, code_iso_char = EXCLUDED.code_iso_char;")

    # Handle FT_POSTING_F with temporary table
    sql_query(conn, "DROP TABLE IF EXISTS templ1 CASCADE;")
    sql_query(conn, '''CREATE TABLE templ1 (
                        oper_date DATE NOT NULL,
                        credit_account_rk NUMERIC NOT NULL,
                        debet_account_rk NUMERIC NOT NULL,
                        credit_amount FLOAT,
                        debet_amount FLOAT);''')
    logging.info("Временная таблица templ1 успешно создана в PostgreSQL")

    insert_data('FT_POSTING_F', tables.posting,
                "INSERT INTO templ1 (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount) VALUES (%s, %s, %s, %s, %s);")

    sql_query(conn, '''INSERT INTO DS.FT_POSTING_F 
                       (SELECT oper_date, credit_account_rk, debet_account_rk, SUM(credit_amount), SUM(debet_amount) 
                        FROM templ1 
                        GROUP BY oper_date, debet_account_rk, credit_account_rk) 
                       ON CONFLICT DO NOTHING;''')
    logging.info("Данные в таблицу FT_POSTING_F добавлены")

    # Handle MD_EXCHANGE_RATE_D with temporary table
    sql_query(conn, "DROP TABLE IF EXISTS templ2 CASCADE;")
    sql_query(conn, '''CREATE TABLE templ2 (
                        data_actual_date DATE NOT NULL,
                        data_actual_end_date DATE,
                        currency_rk NUMERIC NOT NULL,
                        reduced_cource FLOAT,
                        code_iso_num VARCHAR(3));''')
    logging.info("Временная таблица templ2 успешно создана в PostgreSQL")

    insert_data('MD_EXCHANGE_RATE_D', tables.exchange_rate,
                "INSERT INTO templ2 (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num) VALUES (%s, %s, %s, %s, %s);")

    sql_query(conn, '''INSERT INTO DS.MD_EXCHANGE_RATE_D 
                       (SELECT data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num 
                        FROM templ2 
                        GROUP BY data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num) 
                       ON CONFLICT DO NOTHING;''')
    logging.info("Данные в таблицу MD_EXCHANGE_RATE_D добавлены")

    insert_data('MD_LEDGER_ACCOUNT_S', tables.ledger_account,
                "INSERT INTO DS.MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, characteristic, is_resident, is_reserve, is_reserved, is_loan, is_reserved_assets, is_overdue, is_interest, pair_account, start_date, end_date, is_rub_only, min_term, min_term_measure, max_term, max_term_measure, ledger_acc_full_name_translit, is_revaluation, is_correct) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (ledger_account, start_date) DO UPDATE SET chapter = EXCLUDED.chapter, chapter_name = EXCLUDED.chapter_name, section_number = EXCLUDED.section_number, section_name = EXCLUDED.section_name, subsection_name = EXCLUDED.subsection_name, ledger1_account = EXCLUDED.ledger1_account, ledger1_account_name = EXCLUDED.ledger1_account_name, ledger_account_name = EXCLUDED.ledger_account_name, characteristic = EXCLUDED.characteristic, is_resident = EXCLUDED.is_resident, is_reserve = EXCLUDED.is_reserve, is_reserved = EXCLUDED.is_reserved, is_loan = EXCLUDED.is_loan, is_reserved_assets = EXCLUDED.is_reserved_assets, is_overdue = EXCLUDED.is_overdue, is_interest = EXCLUDED.is_interest, pair_account = EXCLUDED.pair_account, end_date = EXCLUDED.end_date, is_rub_only = EXCLUDED.is_rub_only, min_term = EXCLUDED.min_term, min_term_measure = EXCLUDED.min_term_measure, max_term = EXCLUDED.max_term, max_term_measure = EXCLUDED.max_term_measure, ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit, is_revaluation = EXCLUDED.is_revaluation, is_correct = EXCLUDED.is_correct;")


def etl_process_start():
    logging.info("Начинается загрузка данных")
    with get_connection() as conn:
        log_event(conn, "Начинается загрузка данных")
        data_loading(conn)
        log_event(conn, "Загрузка данных завершена")
    logging.info("Загрузка данных завершена")


if __name__ == "__main__":
    etl_process_start()
