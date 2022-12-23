from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

files = Path('.') / 'Files'
each_sheet = {}
df = pd.DataFrame()

for file in files.iterdir():
    reader = pd.ExcelFile(file)
    sheets = [i for i in reader.sheet_names if i.endswith('Daily')]
    for sheet in sheets:
        each_sheet[sheet] = pd.read_excel(reader,sheet_name=sheet)
        df2 = each_sheet[sheet]
        df = pd.concat([df,df2])
        
melted_df = pd.melt(df,id_vars= df.loc[:,~df.columns.str.contains('/')],
                    value_vars=df.loc[:,df.columns.str.contains('/')],
                    value_name='Rate',
                    var_name="ROE"
                    )
melted_df = melted_df.loc[:,~melted_df.columns.str.contains('^Unnamed')]
melted_df.dropna(how='any',inplace=True,subset=['DATE'])
melted_df['DATE'] = pd.to_datetime(melted_df['DATE'])
melted_df['from_curr'] = melted_df['ROE'].str.strip().str[:3]
melted_df['to_curr'] = melted_df['ROE'].str.strip().str[4:]

melted_df.rename(columns={'Month':'MonthNo.', 'Month A':'Month'},inplace=True)
melted_df.dropna(how='any',inplace=True,subset=['Rate'])
my_conn = create_engine("mysql+mysqlconnector://root:pancake@localhost/mis_db")
melted_df.to_sql(con=my_conn,name='roe_table',if_exists='replace',index=False)


# db = con.connect(host='localhost', user='root', password='pancake',database='mis_db')
# curr = db.cursor()

# curr.execute('SELECT COUNT(DATE) AS COUNT FROM roe')

# count = curr.fetchone()

# print(count)