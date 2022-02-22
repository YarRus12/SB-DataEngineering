#!/usr/bin/python
import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)


def report():
    curs.execute("""
    INSERT INTO de1m.RUSS_REP_FRAUD ( -- вставтляем в отчетную таблицу
	event_dt, passport, fio, phone, event_type, report_dt
    )
---- создаем промежуточную выборку данных о трансакциях клиентов
    with tbl AS(
    SELECT 
        trans_date as event_dt,
        fio,
        t.card_num,
        account_num,
        valid_to as account_valid_to,
        passport_num,
        passport_valid_to,
        phone
    FROM DE1M.RUSS_DWH_FACT_TRANSACTIONS t --- ко всем транзакциям присоединяем данные о клиентах 
    LEFT JOIN (SELECT 
                c.first_name || ' ' || c.last_name || ' ' || c.patronymic as fio,
                c.passport_num,
                c.passport_valid_to,
                c.phone,
                a.account_num,
                a.valid_to,
                k.card_num
    FROM RUSS_DWH_DIM_CLIENTS_HIST c
    LEFT JOIN RUSS_DWH_DIM_ACCOUNTS_HIST a
    ON a.client = c.client_id
    LEFT JOIN RUSS_DWH_DIM_CARDS_HIST k
    ON a.account_num = k.account_num
    WHERE 1 = 1
    and c.deleted_flg = 'N' and (current_date BETWEEN c.effective_from and c.effective_to) -- проверка актуальности
    and a.deleted_flg = 'N' and (current_date BETWEEN a.effective_from and a.effective_to) -- проверка актуальности
    and k.deleted_flg = 'N' and (current_date BETWEEN k.effective_from and k.effective_to) -- проверка актуальности
    ) a
    ON t.card_num = a.card_num
    )
-------Выборка завершена    
    SELECT
        event_dt, passport_num, fio, phone, event_type, current_date 
        FROM (
        SELECT 
            event_dt,
            passport_num ,
            fio,
            phone,
            case 
            WHEN passport_valid_to < current_date or passport_num in (SELECT passport_num FROM RUSS_DWH_FACT_PSSPRT_BLCKLST) Then 1
            WHEN account_valid_to < current_date Then 2
        -------Сюда добавим 3 и 4 условие если додумаемся  
            END as event_type
        FROM tbl
        WHERE 1 = 1
                and (1 = 0
                or passport_valid_to < current_date or passport_num in (SELECT passport_num FROM RUSS_DWH_FACT_PSSPRT_BLCKLST)
                or account_valid_to < current_date)
                and event_dt > coalesce( 
                    (select max_update_dt
                    from RUSS_META_COMPLITE
                    where schema_name = 'DE1M' and table_name = 'REPORT'), to_date('1800.01.01', 'YYYY.MM.DD' ))
                    )
    """)
    curs.execute("""
    --обновляем мету, дабы не плодить дубли
    merge into de1m.RUSS_META_COMPLITE trg
    using ( select 'DE1M' schema_name, 'REPORT' table_name, ( select max( event_dt ) from de1m.RUSS_REP_FRAUD ) max_update_dt from dual ) src
    on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
    when matched then 
        update set trg.max_update_dt = src.max_update_dt
        where src.max_update_dt is not null
    when not matched then 
        insert ( schema_name, table_name, max_update_dt )
        values ( 'DE1M', 'REPORT', coalesce( src.max_update_dt,  current_date) )
        """)
    conn.commit()
