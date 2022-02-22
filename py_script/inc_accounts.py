#!/usr/bin/python
import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

def accounts():
    # Выполняем операции инкреентального захвата данных о банковских счетах
    curs.execute("""-- 1. Загрузка в RUSS_STG_ACCOUNTS (захват, extract)
    
    insert into de1m.RUSS_STG_ACCOUNTS (
        account,
        valid_to,
        client,
        create_dt,
        update_dt
        )
    select
        CAST(account AS VARCHAR2(20)),
        CAST(coalesce (valid_to, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE),
        CAST(client AS VARCHAR2(20)),
        CAST(coalesce (create_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE), -- на случай если заливается запись без указания даты создания
        CAST(coalesce (update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE)    
    from BANK.ACCOUNTS
    where coalesce(update_dt, create_dt) 
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'BANK' and table_name = 'ACCOUNTS'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """)
    conn.commit()
    curs.execute("""
    -- 2. Выделение вставок и изменений (transform) вставка в их приемник (load)
    merge into DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST tgt
    using de1m.RUSS_STG_ACCOUNTS stg
    on( stg.account = tgt.account_num and deleted_flg = 'N' )
    when matched then 
        update set tgt.effective_to = stg.update_dt - interval '1' second
        where 1=1
            and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
            and (1=0 
                or stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )
                or stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null )
                )
    when not matched then 
        insert ( 
        account_num,
        valid_to,
        client,
        effective_from,
        effective_to,
        deleted_flg) 
        values ( -- пока оставим до консультации
                CAST(stg.account AS VARCHAR2(20)), 
                CAST(stg.valid_to AS DATE),
                CAST(stg.client AS VARCHAR2(20)),
                CAST(stg.update_dt AS DATE),
                to_date( '31.12.9999', 'DD.MM.YYYY' ),
                CAST('N' AS CHAR(1))            
    )
    """)
    conn.commit()
    curs.execute("""
    insert into DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST
        ( 
        account_num,
        valid_to,
        client,
        effective_from,
        effective_to,
        deleted_flg
        ) 
    select -- пока оставим для консультации
        CAST(stg.account AS VARCHAR2(20)), 
        CAST(stg.valid_to AS DATE),
        CAST(stg.client AS VARCHAR2(20)),
        CAST(stg.update_dt AS DATE),
        to_date( '31.12.9999', 'DD.MM.YYYY' ),
        CAST('N' AS CHAR(1)) 
    from DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST tgt
    inner join DE1M.RUSS_STG_ACCOUNTS stg
    on ( stg.account = tgt.account_num and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where 1=0 -- 
            or stg.account <> tgt.account_num or ( stg.account is null and tgt.account_num is not null ) or ( stg.account is not null and tgt.account_num is null )
    """)
    conn.commit()
    curs.execute("""
    -- 3. Обработка удалений. -- Эти данные в обеих таблицах скастовались. поэтому кастовать все наверное лишнее
    insert into DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST ( 
        account_num,
        valid_to,
        client,
        effective_from,
        effective_to,
        deleted_flg 
        ) 
    select
        tgt.account_num,
        tgt.valid_to,
        tgt.client,
        current_date, 
        to_date( '31.12.9999', 'DD.MM.YYYY' ), 
        CAST('Y' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST tgt
    left join DE1M.RUSS_STG_ACCOUNTS stg
    on ( stg.account = tgt.account_num and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where stg.account is null
    """)
    conn.commit()
    curs.execute("""
    
    update DE1M.RUSS_DWH_DIM_ACCOUNTS_HIST tgt
    set effective_to = current_date - interval '1' second
    where tgt.account_num not in (select account from DE1M.RUSS_STG_ACCOUNTS)
    and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
    """)
    conn.commit()
    curs.execute("""
    -- 4. Обновление метаданных.
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'BANK' schema_name, 'ACCOUNTS' table_name, ( select max( update_dt ) from de1m.RUSS_STG_ACCOUNTS ) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then 
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then 
        insert ( schema_name, table_name, max_update_dt )
        values ( 'BANK', 'ACCOUNTS', coalesce( src.max_update_dt,  to_date( '01.01.1899', 'DD.MM.YYYY' ) ) )
    """)
    conn.commit()
