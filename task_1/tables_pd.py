
import pandas as pd
import task_1.files_paths as files

balance = pd.read_csv(files.balance_csv, keep_default_na=False, delimiter=';')
balance.columns = ['ON_DATE', 'ACCOUNT_RK', 'CURRENCY_RK', 'BALANCE_OUT']

posting = pd.read_csv(files.posting_csv, keep_default_na=False, delimiter=';')
posting.columns = ['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK', 'CREDIT_AMOUNT', 'DEBET_AMOUNT']

account = pd.read_csv(files.account_csv, keep_default_na=False, delimiter=';')
account.columns = ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'ACCOUNT_RK', 'ACCOUNT_NUMBER', 'CHAR_TYPE',
                   'CURRENCY_RK', 'CURRENCY_CODE']

currency = pd.read_csv(files.currency_csv, delimiter=';', keep_default_na=False, encoding='cp866')
currency.columns = ['CURRENCY_RK', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_CODE',
                    'CODE_ISO_CHAR']

exchange_rate = pd.read_csv(files.exchange_rate_csv, keep_default_na=False, delimiter=';')
exchange_rate.columns = ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_RK', 'REDUCED_COURCE',
                         'CODE_ISO_NUM']

ledger_account = pd.read_csv(files.ledger_account_csv, keep_default_na=False, delimiter=';', encoding='utf-8')
required_columns = [
    'CHAPTER', 'CHAPTER_NAME', 'SECTION_NUMBER', 'SECTION_NAME', 'SUBSECTION_NAME',
    'LEDGER1_ACCOUNT', 'LEDGER1_ACCOUNT_NAME', 'LEDGER_ACCOUNT', 'LEDGER_ACCOUNT_NAME',
    'CHARACTERISTIC', 'IS_RESIDENT', 'IS_RESERVE', 'IS_RESERVED', 'IS_LOAN',
    'IS_RESERVED_ASSETS', 'IS_OVERDUE', 'IS_INTEREST', 'PAIR_ACCOUNT', 'START_DATE',
    'END_DATE', 'IS_RUB_ONLY', 'MIN_TERM', 'MIN_TERM_MEASURE', 'MAX_TERM',
    'MAX_TERM_MEASURE', 'LEDGER_ACC_FULL_NAME_TRANSLIT', 'IS_REVALUATION', 'IS_CORRECT'
]

# Добавляем в колонки, которых не существует в файле, значения null
for column in required_columns:
    if column not in ledger_account.columns:
        ledger_account[column] = None


ledger_account = ledger_account[required_columns]

# Save the updated DataFrame for verification (optional)
# ledger_account.to_csv('path_to_save_updated_ledger_account_file.csv', index=False)