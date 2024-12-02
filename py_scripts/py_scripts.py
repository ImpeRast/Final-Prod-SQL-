import pandas as pd
#pip install Pyarrow (иначе дропает ошибку)
import psycopg2
import os
import shutil
import pathlib
import re
import tkinter as tk
from tkinter import simpledialog
from psycopg2 import OperationalError
from sqlalchemy import create_engine


def num_of_starts():
    root = tk.Tk()
    root.withdraw()
    while True:
        user_input = simpledialog.askinteger("Введите число", "Укажите порядковый номер запуска программы от 1-до 3-х", minvalue=1, maxvalue=3)
        if user_input in range(1, 4):
            print("Num of starts: " + str(user_input))
            break
        else:
            continue
    counter = user_input
    return counter

def use_conn_config():
    """reading and filling connecting into a dictionarry 'conf' - configurations from conn_config.txt"""
    config_path = os.path.join(os.getcwd(), 'py_scripts\\conn_config.txt')
    with open(config_path) as configuations:
        conf = {}
        for line in configuations:
            key, value = line.strip().split(' : ')
            conf[key] = value
        return conf

def create_conn(configurations):
    """ create connection to DB using 'conn_config.txt' """
    conn = None
    try:
        conn = psycopg2.connect(**configurations)
        print('Connection to PostgreSQL DB successful')
    except OperationalError as e:
        print(f'The error "{e}" occurred')
    return conn

""" Doesn't work
def find_and_sort_files():
    '' find passport_black_list/ transactions/ terminals in current dict and sort them''
    ls_of_passport_bl = []
    ls_of_terminals = []
    ls_of_transactions = []
    pattern1 = re.compile(r'^(passport_blacklist)_\d{8}.(xlsx|txt)$')
    pattern2 = re.compile(r'^(transactions)_\d{8}.(xlsx|txt)$')
    pattern3 = re.compile(r'^(terminals)_\d{8}.(xlsx|txt)$')
    pattern_list = [pattern1, pattern2, pattern3]
    for filename in os.listdir(os.getcwd()):
        for pattern in pattern_list:
            if  pattern == pattern1 and pattern.match(filename):
                ls_of_passport_bl.append(os.path.abspath(filename))
                ls_of_passport_bl.sort()
            if  pattern == pattern2 and pattern.match(filename):
                ls_of_transactions.append(os.path.abspath(filename))
                ls_of_transactions.sort()
            if pattern == pattern3 and pattern.match(filename):
                ls_of_terminals.append(os.path.abspath(filename))
                ls_of_terminals.sort()
    return ls_of_passport_bl, ls_of_transactions, ls_of_terminals
"""

def execute_query(conn, query):
    """ cursor creating and executing query"""
    conn.autocommit = True
    global cursor
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        print('Querry executed')
    except OperationalError as e:
        print(f'The error "{e}" occured')

def read_ddl_dml_sql(conn, sql_file=(os.path.join(os.getcwd(), 'sql_scripts\\ddl_dml.sql'))):
    #conn.autocommit = True
    with open(sql_file, encoding='UTF-8') as dds:
        dds = dds.read()
        sql_comands = dds.split(';')
        for command in sql_comands:
            try:
                cursor.execute(command)
            except OperationalError as e:
                print(f'The error "{e}" occurred')



""" EXCEL-operations block"""
def execute_pbl_xlsx(conn, file_name):
    """ insert data from xlsx-file format (from passport_blacklist to tmp_passport_blacklist)"""
    data = pd.read_excel(file_name)
    for i in range(len(data)):
        block_date = data.loc[i, 'date']
        bl_num_pas = data.loc[i, 'passport']
        cursor.execute(
        """
            insert  into STG_tmp_passport_blacklist(passport_num, entry_dt)
            values(%s, %s)
        """, [bl_num_pas, block_date]
        )
    pass

def execute_term_xlsx(conn, file_name):
    """ insert data from xlsx-file format (from passport_blacklist to tmp_terminals)"""
    data = pd.read_excel(file_name)
    for i in range(len(data)):
        term_id = data.loc[i, 'terminal_id']
        term_type = data.loc[i, 'terminal_type']
        term_city = data.loc[i, 'terminal_city']
        term_address = data.loc[i, 'terminal_address']
        cursor.execute("""
            insert  into STG_tmp_terminals(terminal_id, terminal_type, terminal_city, terminal_address)
            values(%s, %s, %s, %s)
        """, [term_id, term_type, term_city,  term_address]
        )


def execute_trans_CSV(conf, file_name, table_name='stg_tmp_transactions', shema_name='final_prod'):
    """ Create table using data from CSV-file format (for transactions)"""
    df = pd.read_csv(file_name, sep=';')
    df = df.rename(columns={'transaction_id' : 'trans_id', 'transaction_date' : 'trans_date',
        'amount' : 'amt'
        })
    df.replace(to_replace=',', value='.', inplace=True, limit=None, regex=True)
    engine = create_engine(
        f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
    df.to_sql(name=table_name, con=engine, schema=shema_name, if_exists='append', index=False)
    pass

def file_processing(file_name):
    """ renames used files and places an archive in the directory"""
    if not os.path.exists('archive'):
        os.makedirs('archive')
        print(f'Catalog archive is created.')
    else:
        print(f'Catalog archive is already exists.')

    archive_path = os.path.join(os.getcwd(), 'archive')
    new_file_name = shutil.move(file_name, f'{file_name}' + '.backup')
    if os.path.exists(os.path.join(archive_path, new_file_name)):
        os.remove(os.path.join(archive_path, new_file_name))

    shutil.move(new_file_name, archive_path)
    print(f'file {file_name} removed to archive')
    pass


def del_all_tmp_tables(querry):
    """dlt all tmp tables and views"""
    cursor.execute(querry)
    print('All tmp tables are deleted!')
    pass

