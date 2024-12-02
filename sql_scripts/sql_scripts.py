
create_shcema = 'create schema if not exists final_prod'
set_path = 'set search_path to final_prod'

""" Below querry for creating 6 general tables if not exists:
    transactions, cards, accounts, clients, terminals and passport_blacklist """

delete_tmp_1st = """
    drop table if exists STG_tmp_passport_blacklist;
    drop table if exists STG_tmp_transactions;
    drop table if exists STG_tmp_terminals;
"""
delete_tmp_views = """
    drop view STG_v_transactions;
    drop view STG_v_terminals;
    drop view STG_v_passport_blacklist;
    drop view STG_ft_view;
"""

delete_tmp_2nd = """    
    drop table if exists STG_tmp_passport_blacklist_new_rows;
    drop table if exists STG_tmp_transactions_new_rows;
    drop table if exists STG_tmp_terminals_new_rows;
    
    drop table if exists STG_tmp_passport_blacklist_deleted_rows;
    drop table if exists STG_tmp_transactions_deleted_rows;
    drop table if exists STG_tmp_terminals_deleted_rows;
    
    drop table if exists STG_tmp_passport_blacklist_updated_rows;
    drop table if exists STG_tmp_transactions_updated_rows;
    drop table if exists STG_tmp_terminals_updated_rows;
    
    drop table if exists STG_tmp_ft_type_1;
    drop table if exists STG_tmp_ft_type_2;
    drop table if exists STG_tmp_ft_type_3;

"""


create_tables = """
            CREATE TABLE if not exists DWH_FACT_transactions(
                id serial4,
                trans_id varchar(12),
                trans_date timestamp,
                card_num varchar(20),
                oper_type varchar(128),
                amt decimal,
                oper_result varchar(128),
                terminal varchar(128),
                effective_from timestamp default date_trunc('second', current_timestamp),
                effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
                deleted_flg integer default 0
            );

            CREATE TABLE if not exists DWH_DIM_terminals_HIST(
                id serial4,
                terminal_id varchar(128),
                terminal_type varchar(128),
                terminal_city varchar(128),
                terminal_address varchar(128),
                effective_from timestamp default current_timestamp,
                effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
                deleted_flg integer default 0
            );
            
            CREATE TABLE if not exists DWH_FACT_passport_blacklist(
                id serial4,
                passport_num varchar(128),
                entry_dt date,
                effective_from timestamp default current_timestamp,
                effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
                deleted_flg integer default 0
            )
    """;

""" create temporary tables for 3 SCD2 tables (passport_blacklist, transactions, terminals) """
create_tmp_tables = """ 
    CREATE TABLE if not exists STG_tmp_passport_blacklist(
        passport_num varchar(128),
        entry_dt date,
        effective_from timestamp default current_timestamp,
        effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
        deleted_flg integer default 0
    );
    
    CREATE TABLE if not exists stg_tmp_transactions (
        trans_id varchar(12),
        trans_date timestamp,
        card_num varchar(20),
        oper_type varchar(128),
        amt decimal,
        oper_result varchar(128),
        terminal varchar(128),
        effective_from timestamp default current_timestamp,
        effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
        deleted_flg integer default 0 
    );
    
    CREATE TABLE if not exists STG_tmp_terminals(
        terminal_id varchar(128),
        terminal_type varchar(128),
        terminal_city varchar(128),
        terminal_address varchar(128),
        effective_from timestamp default current_timestamp,
        effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
        deleted_flg integer default 0
    )
    """

""" This is the realisation of Incremental_load for tables 'terminals', 'passport_blacklist',
     and 'transactions'.
     Names of created tables by func: {table_name}_new_rows, {table_name}_deleted_rows,
      {table_name}_changed_rows.
    """
inc_views = """
    CREATE VIEW STG_v_passport_blacklist as
        SELECT
            passport_num, entry_dt
            FROM DWH_FACT_passport_blacklist           
            WHERE deleted_flg = 0
            AND current_timestamp between effective_from and effective_to;
            
    CREATE VIEW STG_v_transactions as
        SELECT
            trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
            FROM DWH_FACT_transactions          
            WHERE deleted_flg = 0
            AND current_timestamp between effective_from and effective_to;
    
    CREATE VIEW STG_v_terminals as
        SELECT
            terminal_id, terminal_type, terminal_city, terminal_address
            FROM DWH_DIM_terminals_HIST           
            WHERE deleted_flg = 0
            AND current_timestamp between effective_from and effective_to;
"""

inc_tables1 = """    
    CREATE TABLE if not exists STG_tmp_passport_blacklist_new_rows as(
            select
                t1.*
            from STG_tmp_passport_blacklist t1
            left join STG_v_passport_blacklist t2
            on t1.passport_num = t2.passport_num
            WHERE t2.passport_num is NULL
    );
    
    CREATE TABLE if not exists STG_tmp_transactions_new_rows as(
            select
                t1.*
            from STG_tmp_transactions t1
            left join STG_v_transactions t2
            on t1.trans_id = t2.trans_id
            WHERE t2.trans_id is NULL
    );
    
    CREATE TABLE if not exists STG_tmp_terminals_new_rows as(
            select
                t1.*
            from STG_tmp_terminals t1
            left join STG_v_terminals t2
            on t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is NULL
            
    );
"""
inc_tables2 = """   
    CREATE TABLE if not exists STG_tmp_passport_blacklist_deleted_rows as(
        select
            t1.*
            from STG_v_passport_blacklist t1
            left join STG_tmp_passport_blacklist t2
            on t1.passport_num = t2.passport_num
            where t2.passport_num is null
    );
        
    CREATE TABLE if not exists STG_tmp_transactions_deleted_rows as(
        select
            t1.*
            from STG_v_transactions t1
            left join STG_tmp_transactions t2
            on t1.trans_id = t2.trans_id   
            where t2.trans_id is null
    );

    CREATE TABLE if not exists STG_tmp_terminals_deleted_rows as(
        select
            t1.*
            from STG_v_terminals t1
            left join STG_tmp_terminals t2
            on t1.terminal_id = t2.terminal_id
            where t2.terminal_id is null
    );
"""
inc_tables3 = """
    CREATE TABLE if not exists STG_tmp_passport_blacklist_updated_rows as(
        select
        t2.*
        from STG_v_passport_blacklist t1
        inner join STG_tmp_passport_blacklist t2
        on t1.passport_num = t2.passport_num
        and (t1.entry_dt = t2.entry_dt)
    );
        
    CREATE TABLE if not exists STG_tmp_transactions_updated_rows as(
        select
        t2.*
        from STG_v_transactions t1
        inner join STG_tmp_transactions t2
        on t1.trans_id = t2.trans_id
        and (t1.trans_date <> t2.trans_date
            or t1.card_num <> t2.card_num
            or t1.oper_type <> t2.oper_type
            or t1.amt <> t2.amt
            or t1.oper_result <> t2.oper_result
            or t1.terminal <> t2.terminal)
    );
            
    CREATE TABLE if not exists STG_tmp_terminals_updated_rows as(
        select
        t2.*
        from STG_v_terminals t1
        inner join STG_tmp_terminals t2
        on t1.terminal_id = t2.terminal_id
        and (t1.terminal_type <> t2.terminal_type
            or t1.terminal_city <> t2.terminal_city
            or t1.terminal_address <> t2.terminal_address)
    );
"""
inc_insert1 = """
    INSERT INTO DWH_FACT_passport_blacklist(
            passport_num, entry_dt
        )
        SELECT
            passport_num, entry_dt
        FROM STG_tmp_passport_blacklist_new_rows
    ;
    
    INSERT INTO DWH_FACT_transactions(
            trans_id, trans_date, card_num,	oper_type,	amt,
            	oper_result,	terminal
            )
        SELECT
            trans_id, trans_date, card_num,	oper_type,	amt,
            	oper_result,	terminal
        FROM STG_tmp_transactions_new_rows    
    ;
    
    INSERT INTO DWH_DIM_terminals_HIST(
            terminal_id,	terminal_type,	terminal_city,
            	terminal_address
            )
        SELECT
            terminal_id,	terminal_type,	terminal_city,
            	terminal_address
        FROM STG_tmp_terminals_new_rows
    ;
"""


inc_update1 = """
    UPDATE DWH_FACT_passport_blacklist
        set effective_to = date_trunc('second', now() - interval '1 second'),
        deleted_flg = 1
        where passport_num in (select passport_num from STG_tmp_passport_blacklist_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;
        
    UPDATE DWH_FACT_transactions 
        set effective_to = date_trunc('second', now() - interval '1 second'),
        deleted_flg = 1
        where trans_id in (select trans_id from STG_tmp_transactions_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;

    UPDATE DWH_DIM_terminals_HIST
        set effective_to = date_trunc('second', now() - interval '1 second'),
        deleted_flg = 1
        where terminal_id in (select terminal_id from STG_tmp_terminals_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;
"""
inc_insert2 = """
    INSERT INTO DWH_FACT_passport_blacklist(
            passport_num, entry_dt, deleted_flg
        )
        SELECT
            passport_num, entry_dt, deleted_flg
        FROM STG_tmp_passport_blacklist_updated_rows
    ;
    
    INSERT INTO DWH_FACT_transactions(
            trans_id, trans_date, card_num,	oper_type,	amt,
            	oper_result,	terminal, deleted_flg
            )
        SELECT
            trans_id, trans_date, card_num,	oper_type,	amt,
            	oper_result,	terminal, deleted_flg
        FROM STG_tmp_transactions_updated_rows;
    
    INSERT INTO DWH_DIM_terminals_HIST(
            terminal_id,	terminal_type,	terminal_city,
            	terminal_address, deleted_flg
            )
        SELECT
            terminal_id,	terminal_type,	terminal_city,
            	terminal_address, deleted_flg
        FROM STG_tmp_terminals_updated_rows      
    ;
"""
inc_update2 = """
    UPDATE
        DWH_FACT_passport_blacklist
        set
        effective_to = date_trunc('second', now() - interval
        '1 second'),
        deleted_flg = 1
        where
        passport_num in (select passport_num from STG_tmp_passport_blacklist_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;

    UPDATE
        DWH_FACT_transactions
        set
        effective_to = date_trunc('second', now() - interval
        '1 second'),
        deleted_flg = 1
        where
        trans_id in (select trans_id from STG_tmp_transactions_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;

    UPDATE
        DWH_DIM_terminals_HIST
        set
        effective_to = date_trunc('second', now() - interval
        '1 second'),
        deleted_flg = 1
        where
        terminal_id in (select terminal_id from STG_tmp_terminals_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    ;      
"""

"""fraudulent transactions block
type1 - any operations with pasport in black_list with an expired passport
type2 - any operations under an ineffective contract
type3 - perform transactions in different cities within one hour
type4 - Trying to select the amount. More than 3 times pass within 20 minutes
    with the following pattern: each subsequent one is smaller than the previous one,
    (wherein)
    All except the last one were rejected. Last operation (successful) in such a chain considered fraudulent.
"""

crt_data_mart = """
    CREATE TABLE IF NOT EXISTS REP_FRAUD(
        event_dt timestamp,
        passport varchar(128),
        fio varchar(128),
        phone varchar(128),
        event_type varchar(120),
        report_dt timestamp default current_timestamp
    );
    """
ft_view ="""
    CREATE VIEW stg_ft_view as(
        select
            t3.passport_num as passport,
            (select concat (t3.last_name, ' ', t3.first_name, ' ', t3.patronymic)) as fio,
            t3.phone,
            (current_timestamp) as report_dt,
            t1.card_num,
            t2.valid_to,
            t3.passport_valid_to
        from DWH_DIM_cards t1
        inner join DWH_DIM_accounts t2
            on t1.account_num = t2.account_num
        inner join DWH_DIM_clients t3
            on t3.client_id = t2.client
        );
"""


ft_type_1 = """
    CREATE TABLE if not exists STG_tmp_ft_type_1 as (
    select 
		t2.trans_date as event_dt,
		t1.passport,
		t1.fio,
		t1.phone,
		t1.report_dt,
		('ft_type_1') as event_type 
	from STG_ft_view t1
		inner join DWH_FACT_transactions t2
		on t1.card_num = t2.card_num
	where t2.trans_date > t1.valid_to and t2.oper_result = 'SUCCESS' and t2.deleted_flg = '0'
	or (t1.passport in (select passport_num from DWH_FACT_passport_blacklist) and t2.oper_result = 'SUCCESS')
);
 
    insert into rep_fraud(
 		event_dt,	passport,	fio,	phone,		report_dt, event_type
 		)
 	select
 		event_dt,	passport,	fio,	phone,		report_dt, event_type
 	from STG_tmp_ft_type_1;
 	
    ;
"""

ft_type_2 = """
CREATE TABLE if not exists STG_tmp_ft_type_2 as (
    select 
		t2.trans_date as event_dt,
		t1.passport,
		t1.fio,
		t1.phone,
		t1.report_dt,
		('ft_type_2') as event_type 
	from STG_ft_view t1
		inner join DWH_FACT_transactions t2
		on t1.card_num = t2.card_num
	where t2.trans_date > t1.valid_to and t2.oper_result = 'SUCCESS' and t2.deleted_flg = '0'
	);
	
    insert into rep_fraud(
 		event_dt,	passport,	fio,	phone,	event_type ,	report_dt
 		)
 	select
 		event_dt,	passport,	fio,	phone,	event_type ,	report_dt
 	from STG_tmp_ft_type_2;
 	
    ;
"""


ft_type_3 = """
    CREATE TABLE if not exists STG_tmp_ft_type_3 as (
        select distinct
        t2.trans_date as event_dt,
        t1.passport,
        t1.fio,
        t1.phone,
        t1.report_dt,
        ('ft_type_3') as event_type 
    from STG_ft_view t1
    inner join (SELECT t1.card_num, t1.trans_date, t2.trans_date AS transaction_date2,
            ter1.terminal_city, ter2.terminal_city, t1.oper_result
            FROM DWH_FACT_transactions t1
            JOIN DWH_FACT_transactions t2 ON t1.card_num = t2.card_num
            JOIN DWH_DIM_terminals_HIST ter1 ON t1.terminal = ter1.terminal_id
            JOIN DWH_DIM_terminals_HIST ter2 ON t2.terminal = ter2.terminal_id
            WHERE t1.trans_date > t2.trans_date
            AND t1.trans_date + interval '1 hour' > t2.trans_date
            AND ter1.terminal_city <> ter2.terminal_city) as t2
        on t1.card_num = t2.card_num
    where
        t2.oper_result = 'SUCCESS'
    );
        
    insert into rep_fraud(
 		event_dt,	passport,	fio,	phone,	event_type, 	report_dt
 		)
 	select
 		event_dt,	passport,	fio,	phone,	event_type,	report_dt
 	from STG_tmp_ft_type_3;
 	
"""