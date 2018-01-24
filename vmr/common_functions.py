import pyodbc

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