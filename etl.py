import os
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
uid = os.environ['pguid']
upass = os.environ['pgpass']
server = 'localhost'
driver = '{SQL Server Native Client 11.0}'
database = 'your_db_name'


def extraction():
    try:
        conn = pyodbc.connect(f"DRIVER={driver};SERVER={server}\SQLEXPRESS;DATABASE={database};UID={uid};pwd={upass}")
        cusor = conn.cursor()
        cusor.execute("""select t.name as table_name from 
                     sys.tables t where t.name in ('DimProduct',
                     'DimProductSubcategory','DimProductCategory','DimSalesTerritory',
                     'FactInternetSales')""")
        tables = cusor.fetchall()
        
        for tbl in tables:
            df = pd.read_sql_query(f'select * from {tbl[0]}',conn)
            load(df,tbl[0])

    except Exception as e:
        print(f'error message is {str(e)}')
    
    finally:
        conn.close()


def load(df,tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{upass}@{server}:5432/your_db_name')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)} for tbale {tbl}')

        # save df into postgresql
        df.to_sql(f'stg_{tbl}',engine,if_exists='replace',index=False)

    
    except Exception as e:
        print(f'error message {e}')


try:
    extraction()
except Exception as e:
    print(f'Error {e}')
