#!/usr/bin/python
import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                          'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
                          ['de1m', 'samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)


# Укладка данных о терминалах
def terminals():
    curs.execute("""
    -- 1. Загрузка в RUSS_STG_TERMINALS (захват, extract)

    insert into de1m.RUSS_STG_TERMINALS (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        create_dt,
        update_dt
        )
    select
        CAST(terminal_id AS VARCHAR2(30)), 
        CAST(terminal_type AS VARCHAR2(50)),
        CAST(terminal_city AS VARCHAR2(50)), 
        CAST(terminal_address AS VARCHAR2(100)), 
        CAST(coalesce (create_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE), -- на случай если заливается запись без указания даты создания
        CAST(coalesce (update_dt, coalesce(create_dt,to_date( '1900.01.01', 'YYYY.MM.DD' ))) AS DATE)    
    from DE1M.RUSS_SOURCE_TERMINALS
    where coalesce(update_dt, to_date('1900.01.01', 'YYYY.MM.DD' )) 
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'DE1M' and table_name = 'TERMINALS'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """)
    conn.commit()
    curs.execute("""
    -- 2. Выделение вставок и изменений (transform) вставка в их приемник (load)
    merge into DE1M.RUSS_DWH_DIM_TERMINALS_HIST tgt
    using de1m.RUSS_STG_TERMINALS stg
    on( stg.terminal_id = tgt.terminal_id and deleted_flg = 'N' )
    when matched then
        update set tgt.effective_to = stg.update_dt - interval '1' second
        where 1=1
            and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
            and (1=0 -- 
                or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
                or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
                or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
        )
    when not matched then 
        insert ( 
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from,
        effective_to,
        deleted_flg ) 
        values ( -- пока оставим до консультации
                CAST(stg.terminal_id AS VARCHAR2(30)), 
                CAST(stg.terminal_type AS VARCHAR2(50)),
                CAST(stg.terminal_city AS VARCHAR2(50)), 
                CAST(stg.terminal_address AS VARCHAR2(100)),
                CAST(stg.update_dt AS DATE),
                to_date( '31.12.9999', 'DD.MM.YYYY' ),
                CAST('N' AS CHAR(1))
    )
    """)
    conn.commit()
    curs.execute("""
    insert into DE1M.RUSS_DWH_DIM_TERMINALS_HIST
                ( 
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from,
        effective_to,
        deleted_flg  ) 
    select -- пока оставим для консультации
        CAST(stg.terminal_id AS VARCHAR2(30)), 
        CAST(stg.terminal_type AS VARCHAR2(50)),
        CAST(stg.terminal_city AS VARCHAR2(50)), 
        CAST(stg.terminal_address AS VARCHAR2(100)),
        CAST(stg.update_dt AS DATE),
        to_date( '31.12.9999', 'DD.MM.YYYY' ),
        CAST('N' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_TERMINALS_HIST tgt
    inner join DE1M.RUSS_STG_TERMINALS stg
    on ( stg.terminal_id = tgt.terminal_id and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where 1=0 -- date_of_birth не будем проверять на соответствие
            or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
                or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
                or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )

    """)
    conn.commit()
    curs.execute("""
    -- 3. Обработка удалений.
    insert into DE1M.RUSS_DWH_DIM_TERMINALS_HIST ( 
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from,
        effective_to,
        deleted_flg 
        ) 
    select
        tgt.terminal_id,
        tgt.terminal_type,
        tgt.terminal_city,
        tgt.terminal_address,
        current_date, 
        to_date( '31.12.9999', 'DD.MM.YYYY' ), 
        CAST('Y' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_TERMINALS_HIST tgt
    left join DE1M.RUSS_STG_TERMINALS stg
    on ( stg.terminal_id = tgt.terminal_id and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where stg.terminal_id is null and (stg.terminal_type is not null or stg.terminal_city is not null or stg.terminal_address is not null)
    """)
    conn.commit()
    curs.execute("""
    update DE1M.RUSS_DWH_DIM_TERMINALS_HIST tgt
    set effective_to = current_date - interval '1' second
    where tgt.terminal_id not in (select terminal_id from DE1M.RUSS_STG_TERMINALS)
    and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
    """)
    conn.commit()
    curs.execute("""
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'DE1M' schema_name, 'TERMINALS' table_name, ( select max( update_dt ) from de1m.RUSS_STG_TERMINALS ) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then 
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then 
        insert ( schema_name, table_name, max_update_dt )
        values ( 'DE1M', 'TERMINALS', coalesce( src.max_update_dt,  current_date) )
    """)
    conn.commit()
    curs.execute("""
    TRUNCATE TABLE DE1M.RUSS_SOURCE_TERMINALS -- Очищаем source
    """)
    conn.commit()


def black_pass():
    curs.execute("""
    INSERT INTO DE1M.RUSS_DWH_FACT_PSSPRT_BLCKLST 
    ( passport_num, entry_dt)
    SELECT
        cast (s.passport_num AS VARCHAR2(30)),
        to_date(s.entry_dt, 'YYYY-MM-DD HH24-MI-SS')
    FROM DE1M.RUSS_SOURCE_PSSPRT_BLCKLST s
    where coalesce(update_dt, to_date('1900.01.01', 'YYYY.MM.DD' )) 
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'DE1M' and table_name = 'PSSPRT_BLCKLST'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """)
    curs.execute("""
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'DE1M' schema_name, 'PSSPRT_BLCKLST' table_name, ( select max( entry_dt ) from de1m.RUSS_SOURCE_PSSPRT_BLCKLST) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then 
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then 
        insert ( schema_name, table_name, max_update_dt )
        values ( 'DE1M', 'PSSPRT_BLCKLST', coalesce( src.max_update_dt, to_date( '01.01.1899', 'DD.MM.YYYY' )) )
    """)
    conn.commit()