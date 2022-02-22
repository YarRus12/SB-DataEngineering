import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

def clients():
    # Выполняем операции инкреентального захвата данных о клиентах
    curs.execute("""-- 1. Загрузка в RUSS_STG_CLIENTS (захват, extract)
    insert into de1m.RUSS_STG_CLIENTS (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        create_dt,
        update_dt
        )
    select
        CAST(client_id AS VARCHAR2(20)), 
        CAST(last_name AS VARCHAR2(100)),
        CAST(first_name AS VARCHAR2(100)), 
        CAST(patronymic AS VARCHAR2(100)), 
        CAST(date_of_birth AS DATE),
        CAST(passport_num AS VARCHAR2(15)),
        CAST(coalesce (passport_valid_to, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE),
        CAST(phone AS VARCHAR2(20)),
        CAST(coalesce (create_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE),
        CAST(coalesce (update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' )) AS DATE)    
    from BANK.CLIENTS
    where coalesce(update_dt, create_dt)
            > coalesce( (
        select max_update_dt
        from RUSS_META_COMPLITE
        where schema_name = 'BANK' and table_name = 'CLIENTS'
    ), to_date('1800.01.01', 'YYYY.MM.DD' ))
    """
                 )
    conn.commit()

    curs.execute("""
    -- 2. Выделение вставок и изменений (transform) вставка в их приемник (load)
    merge into DE1M.RUSS_DWH_DIM_CLIENTS_HIST tgt
    using de1m.RUSS_STG_CLIENTS stg
    on( stg.client_id = tgt.client_id and deleted_flg = 'N' )
    when matched then
        update set tgt.effective_to = stg.update_dt - interval '1' second
        where 1=1
            and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
            and (1=0 -- date_of_birth не будем проверять на соответствие
                or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )
                or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
                or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )
                or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
                or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
                or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null )
        )
    when not matched then
        insert (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        effective_from,
        effective_to,
        deleted_flg )
        values ( -- может это и лишнее, зато знаем что заливаем
                CAST(stg.client_id AS VARCHAR2(20)),
                CAST(stg.last_name AS VARCHAR2(100)),
                CAST(stg.first_name AS VARCHAR2(100)),
                CAST(stg.patronymic AS VARCHAR2(100)),
                CAST(stg.date_of_birth AS DATE),
                CAST(stg.passport_num AS VARCHAR2(15)),
                CAST(stg.passport_valid_to AS DATE),
                CAST(stg.phone AS VARCHAR2(20)),
                CAST(stg.update_dt AS DATE),
                to_date( '31.12.9999', 'DD.MM.YYYY' ),
                CAST('N' AS CHAR(1))
    
    )
    """)
    conn.commit()
    curs.execute("""
    insert into DE1M.RUSS_DWH_DIM_CLIENTS_HIST
                (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        effective_from,
        effective_to,
        deleted_flg )
    select -- может это и лишнее, зато знаем что заливаем
        CAST(stg.client_id AS VARCHAR2(20)),
        CAST(stg.last_name AS VARCHAR2(100)),
        CAST(stg.first_name AS VARCHAR2(100)),
        CAST(stg.patronymic AS VARCHAR2(100)),
        CAST(stg.date_of_birth AS DATE),
        CAST(stg.passport_num AS VARCHAR2(15)),
        CAST(stg.passport_valid_to AS DATE),
        CAST(stg.phone AS VARCHAR2(20)),
        CAST(stg.update_dt AS DATE),
        to_date( '31.12.9999', 'DD.MM.YYYY' ),
        CAST('N' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_CLIENTS_HIST tgt
    inner join DE1M.RUSS_STG_CLIENTS stg
    on ( stg.client_id = tgt.client_id and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where 1=0 -- date_of_birth не будем проверять на соответствие
            or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )
            or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
            or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )
            or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
            or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
            or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null )
    """)
    conn.commit()
    curs.execute("""
    -- 3. Обработка удалений.
    insert into DE1M.RUSS_DWH_DIM_CLIENTS_HIST (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        effective_from,
        effective_to,
        deleted_flg
        )
    select
        tgt.client_id,
        tgt.last_name,
        tgt.first_name,
        tgt.patronymic,
        tgt.date_of_birth,
        tgt.passport_num,
        tgt.passport_valid_to,
        tgt.phone,
        current_date,
        to_date( '31.12.9999', 'DD.MM.YYYY' ),
        CAST('Y' AS CHAR(1))
    from DE1M.RUSS_DWH_DIM_CLIENTS_HIST tgt
    left join DE1M.RUSS_STG_CLIENTS stg
    on ( stg.client_id = tgt.client_id and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
    where stg.client_id is null
    """)
    conn.commit()
    curs.execute("""
    update DE1M.RUSS_DWH_DIM_CLIENTS_HIST tgt
    set effective_to = current_date - interval '1' second
    where tgt.client_id not in (select client_id from DE1M.RUSS_STG_CLIENTS)
    and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
    """)
    conn.commit()
    curs.execute("""
    -- 4. Обновление метаданных.
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'BANK' schema_name, 'CLIENTS' table_name, ( select max( update_dt ) from de1m.RUSS_STG_CLIENTS ) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then
        insert ( schema_name, table_name, max_update_dt )
        values ( 'BANK', 'CLIENT', coalesce( src.max_update_dt, to_date( '01.01.1899', 'DD.MM.YYYY' )) )
    """)
    conn.commit()