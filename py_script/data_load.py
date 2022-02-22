#!/usr/bin/python
import pandas as pd
import jaydebeapi
import os

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                          'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
                          ['de1m', 'samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

path = "/home/de1m/russ/data_3_files/"


def data_load(passports, terminals, transactions):
    # Начинаем загрузку списка украденных паспортов в базу данных, в отдельную таблицу source, так как не хочу помещать в основную таблицу дубли паспортов
    passport_path = str(path + passports)
    df_pas = pd.read_excel(passport_path,
                           sheet_name='blacklist', header=0, index_col=None)
    df_pas.columns = ["date", "passport"]
    df_pas = df_pas.astype(str)
    curs.executemany(
        "insert into DE1M.RUSS_SOURCE_PSSPRT_BLCKLST ( entry_dt, passport_num ) values (to_date( ?, 'YYYY-MM-DD'), ? )  ",
        df_pas.values.tolist())
    conn.commit()

    # Начинаем загрузку списка терминалов. Список меняется поэтому делаем это в source и там уже обрабатываем
    terminal_path = str(path + terminals)
    df_ter = pd.read_excel(terminal_path,
                           sheet_name='terminals', header=0, index_col=None)
    df_ter.columns = ["terminal_id", "terminal_type", "terminal_city", "terminal_address"]
    df_ter = df_ter.astype(str)
    curs.executemany(
        "insert into DE1M.RUSS_SOURCE_TERMINALS ( terminal_id, terminal_type, terminal_city, terminal_address) values ( ?, ?, ?, ? ) ",
        df_ter.values.tolist())
    conn.commit()

    # Начинаем загрузку списка всех транзакций непосредственно в хранилище
    transactions_path = str(path + transactions)
    df_trans = pd.read_csv(transactions_path, sep=";", header=0, index_col=None)
    df_trans.columns = ["transaction_id", "transaction_date", "amount", "card_num", "oper_type", "oper_result",
                        "terminal"]
    # обрабатываем номера карт, чтобы убрать нули, и заменяем в amount точку на запятую
    df_trans['card_num'] = [x.replace(' ', '') for x in df_trans['card_num']]
    df_trans['amount'] = [x.replace(',', '.') for x in df_trans['amount']]
    df_trans = df_trans.astype(str)    
    curs.executemany(
        "insert into DE1M.RUSS_DWH_FACT_TRANSACTIONS ( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) values ( cast(? as VARCHAR2(50)), to_date(?,'YYYY-MM-DD HH24:MI:SS'), cast(? as DECIMAL), cast(? as VARCHAR2(50)), cast(? as VARCHAR2(50)), cast(? as VARCHAR2(30)), cast(? as VARCHAR2(50))) ", df_trans.values.tolist())
    conn.commit()