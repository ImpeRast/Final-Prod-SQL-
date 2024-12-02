from py_scripts import py_scripts as ps
from sql_scripts import sql_scripts as ss
import os


conf = ps.use_conn_config() #1
conn = ps.create_conn(conf) #2
ps.execute_query(conn, ss.create_shcema) #3 creating shcema
ps.execute_query(conn, ss.set_path) #4 set path to shcema
ps.execute_query(conn, ss.create_tables) #5 create 3 final tables
ps.execute_query(conn, ss.create_tmp_tables) #7 create 3 tmp tables for 'passport_blacklist,
#terminals' and 'transaction'

counter = ps.num_of_starts() # for choose num of starts (return counter)

if counter == 1:
    ps.read_ddl_dml_sql(conn) #6 create and fill 3 tables from ddl_dml_sql ('cards', 'clients', 'accounts')

#pas_file, trans_file, term_file = ps.find_and_sort_files() #find files to load in bd (STG)
#------------------------------------------------------------------------------


""" This load files into BD for each script start (depending on counter)"""

if counter == 1:
    ps.execute_pbl_xlsx(conn, (os.path.join(os.getcwd(), 'файлы с данными\\passport_blacklist_01032021.xlsx')))
    ps.execute_term_xlsx(conn,(os.path.join(os.getcwd(), 'файлы с данными\\terminals_01032021.xlsx')))
    ps.execute_trans_CSV(conf, (os.path.join(os.getcwd(), 'файлы с данными\\transactions_01032021.txt')))

    ps.file_processing('файлы с данными\\passport_blacklist_01032021.xlsx')
    ps.file_processing('файлы с данными\\terminals_01032021.xlsx')
    ps.file_processing('файлы с данными\\transactions_01032021.txt')

elif counter == 2:
    ps.execute_pbl_xlsx(conn, (os.path.join(os.getcwd(), 'файлы с данными\\passport_blacklist_02032021.xlsx')))
    ps.execute_term_xlsx(conn,(os.path.join(os.getcwd(), 'файлы с данными\\terminals_02032021.xlsx')))
    ps.execute_trans_CSV(conf, (os.path.join(os.getcwd(), 'файлы с данными\\transactions_02032021.txt')))

    ps.file_processing('файлы с данными\\passport_blacklist_02032021.xlsx')
    ps.file_processing('файлы с данными\\terminals_02032021.xlsx')
    ps.file_processing('файлы с данными\\transactions_02032021.txt')

elif counter == 3:
    ps.execute_pbl_xlsx(conn, (os.path.join(os.getcwd(), 'файлы с данными\\passport_blacklist_03032021.xlsx')))
    ps.execute_term_xlsx(conn,(os.path.join(os.getcwd(), 'файлы с данными\\terminals_03032021.xlsx')))
    ps.execute_trans_CSV(conf, (os.path.join(os.getcwd(), 'файлы с данными\\transactions_03032021.txt')))

    ps.file_processing('файлы с данными\\passport_blacklist_03032021.xlsx')
    ps.file_processing('файлы с данными\\terminals_03032021.xlsx')
    ps.file_processing('файлы с данными\\transactions_03032021.txt')


""" This block about incremental loading"""
ps.execute_query(conn, ss.inc_views) #11 create 3 views in DB

ps.execute_query(conn, ss.inc_tables1) #11 complete inc load
ps.execute_query(conn, ss.inc_tables2)
ps.execute_query(conn, ss.inc_tables3)

ps.execute_query(conn, ss.inc_insert1)
ps.execute_query(conn, ss.inc_update1)
ps.execute_query(conn, ss.inc_insert2)
ps.execute_query(conn, ss.inc_update2)

""" fraudulent transactions block """
ps.execute_query(conn, ss.crt_data_mart) # create data mart REPFRAUD
ps.execute_query(conn, ss.ft_view)
ps.execute_query(conn, ss.ft_type_1)
ps.execute_query(conn, ss.ft_type_2)
ps.execute_query(conn, ss.ft_type_3)

""" tmp_drop block """
ps.del_all_tmp_tables(ss.delete_tmp_views)  # delete tmp_'views' tables
ps.del_all_tmp_tables(ss.delete_tmp_1st)  # delete tmp_'name' tables
ps.del_all_tmp_tables(ss.delete_tmp_2nd) # delete other tmp tables
