import pyodbc
import pandas as pd

def access_retrieve_vmr(sql):
    conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=C:\vmr\vmr.accdb;'
    )
    
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close
    return result

def access_update_vmr(sql):
    conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=C:\vmr\vmr.accdb;'
    )
    
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute(sql)
    cur.commit()
    cur.close
    return None

def access_update_many_vmr(sql, insert_list):
    conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=C:\vmr\vmr.accdb;'
    )
    
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.executemany(sql, insert_list)
    cur.commit()
    cur.close
    return None

def d_values(d, depth):
    if depth == 1:
        for i in d.values():
            yield i
    else:
        for v in d.values():
            if isinstance(v, dict):
                for i in d_values(v, depth-1):
                    yield i
                    

def access_retrieve_vmr_pd(sql):
    conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=C:\vmr\vmr.accdb;'
    )    
    conn = pyodbc.connect(conn_str)
    result = pd.read_sql(sql, conn)
    return result                    