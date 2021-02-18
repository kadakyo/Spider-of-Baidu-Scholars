import pandas as pd
import sqlite3

date = '20200524'
con = sqlite3.connect('baiduxueshu.db')
scholars = pd.read_sql('select * from hcp_demo_scholars_%s;' % date, con=con)
essays = pd.read_sql('select * from hcp_demo_essays_%s;' % date, con=con)
scholars.to_csv('hcp_demo_scholars_%s.csv' % date , index=False, encoding='utf-8_sig')
essays.to_csv('hcp_demo_essays_%s.csv' % date, index=False, encoding='utf-8_sig')
