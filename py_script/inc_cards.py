#!/usr/bin/python
import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)
def cards():
    # Выполняем операции инкреентального захвата данных о банковских картах клиентов
    curs.execute("""
    -- 1. Загрузка в RUSS_STG_CARDS(захват, extract)
    
    insert into de1m.RUSS_STG_CARDS (
        card_num,
        account_num,
        create_dt,
        update_dt
        )
    select
        CAST((substr( card_num, 1, instr( card_num,' ')-1)
        ||substr( card_num, 6, instr( card_num,' ')-1)
        ||substr( card_num, 11, instr( card_num,' ')-1)
        ||substr( card_num, 16, instr( card_num,' ')-1)) AS VARCHAR2(20)) as card_num,
        CAST(account AS VARCHAR2(20)),
        CAST(coalesce (create_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE), -- на случай если заливается запись без указания даты создания
        CAST(coalesce (update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE)    
    from BANK.CARDS
    where coalesce(update_dt, create_dt) 
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'BANK' and table_name = 'CARDS'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """)
    conn.commit()
    curs.execute("""
    -- 1. Загрузка в RUSS_STG_CARDS(захват, extract)
    
    insert into de1m.RUSS_STG_CARDS (
        card_num,
        account_num,
        create_dt,
        update_dt
        )
    select
        CAST((substr( card_num, 1, instr( card_num,' ')-1)
        ||substr( card_num, 6, instr( card_num,' ')-1)
        ||substr( card_num, 11, instr( card_num,' ')-1)
        ||substr( card_num, 16, instr( card_num,' ')-1)) AS VARCHAR2(20)) as card_num,
        CAST(account AS VARCHAR2(20)),
        CAST(coalesce (create_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE), -- на случай если заливается запись без указания даты создания
        CAST(coalesce (update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE)    
    from BANK.CARDS
    where coalesce(update_dt, create_dt) 
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'BANK' and table_name = 'CARDS'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """)
    conn.commit()
    curs.execute("""
    -- 2. Выделение вставок и изменений (transform); вставка в их приемник (load)
    merge into DE1M.RUSS_DWH_DIM_CARDS_HIST tgt
    using de1m.RUSS_STG_CARDS stg
    on( stg.card_num = tgt.card_num and deleted_flg = 'N' )
    when matched then 
        update set tgt.effective_to = stg.update_dt - interval '1' second
        where 1=1
            and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
            and (1=0 
                or stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null )
                )
    when not matched then 
        insert ( 
        card_num,
        account_num,
        effective_from,
        effective_to,
        deleted_flg) 
        values ( -- пока оставим до консультации
                CAST(stg.card_num AS VARCHAR2(20)), 
                CAST(stg.account_num AS VARCHAR2(100)),
                CAST(stg.update_dt AS DATE),
                to_date( '31.12.9999', 'DD.MM.YYYY' ),
                CAST('N' AS CHAR(1))            
    )
    """)
    curs.execute("""
    insert into DE1M.RUSS_DWH_DIM_CARDS_HIST
        ( 
        card_num,
        account_num,
        effective_from,
        effective_to,
        deleted_flg
        ) 
    select -- пока оставим для консультации
        CAST(stg.card_num AS VARCHAR2(20)), 
        CAST(stg.account_num AS VARCHAR2(100)),
        CAST(stg.update_dt AS DATE),
        to_date( '31.12.9999', 'DD.MM.YYYY' ),
        CAST('N' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_CARDS_HIST tgt
    inner join DE1M.RUSS_STG_CARDS stg
    on ( stg.card_num = tgt.card_num and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where 1=0 -- 
            or stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null )
    """)
    conn.commit()
    curs.execute("""
    -- 3. Обработка удалений.
    insert into DE1M.RUSS_DWH_DIM_CARDS_HIST ( 
        card_num,
        account_num,
        effective_from,
        effective_to,
        deleted_flg 
        ) 
    select
        tgt.card_num,
        tgt.account_num,
        current_date, 
        to_date( '31.12.9999', 'DD.MM.YYYY' ), 
        CAST('Y' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_CARDS_HIST tgt
    left join DE1M.RUSS_STG_CARDS stg
    on ( stg.card_num = tgt.card_num and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where stg.card_num is null
    """)
    conn.commit()
    curs.execute("""
    update DE1M.RUSS_DWH_DIM_CARDS_HIST tgt
    set effective_to = current_date - interval '1' second
    where tgt.card_num not in (select card_num from DE1M.RUSS_STG_CARDS)
    and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
    """)
    conn.commit()
    curs.execute("""
    -- 4. Обновление метаданных.
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'BANK' schema_name, 'CARDS' table_name, ( select max( update_dt ) from de1m.RUSS_STG_CARDS ) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then 
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then 
        insert ( schema_name, table_name, max_update_dt )
        values ( 'BANK', 'CARDS', coalesce( src.max_update_dt,  to_date( '01.01.1899', 'DD.MM.YYYY' ) ) )
    """)
    conn.commit()