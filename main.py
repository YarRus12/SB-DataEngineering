#!/usr/bin/python
import pandas as pd
import jaydebeapi
import sys
import os

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'], '/home/de1m/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

sys.path.insert(0,'py_script')

files = os.listdir(path="/home/de1m/russ/data_3_files/")
passports = 0
terminals = 0
transactions = 0

for i in files:
    if i[:8] == 'terminal': terminals = i
    elif i[:8] == 'passport': passports = i
    elif i[:8] == 'transact': transactions = i

from data_load import *
data_load(passports,terminals,transactions )

from inc_clients import *
clients()

from inc_accounts import *
accounts()
from inc_cards import *
cards()
from data_transform import *
terminals()
black_pass()
from report import *
report()

print('OPERATION COMPLITE')