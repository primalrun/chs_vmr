import pyodbc
import pandas as pd
import math

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


def benchmark_rank(i_list, clinician_audit, date_to_period, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']
        pd_str = date_to_period[x[1]]
        anlz_f = clinician_audit[x[0]]['anlz_f_' + pd_str]
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)                    
        elif anlz_f != None:
            anlz_amount = x[2] * float(anlz_f)                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank


def rank_calc(anlz_amount, bm_list):
    if bm_list[-1][1] == 0:
        return None
    elif math.isnan(anlz_amount) == True:
        return None
    elif anlz_amount < bm_list[0][1]:
        bm_rank = 10
    elif anlz_amount > bm_list[-1][1]:        
        bm_rank = int((anlz_amount / bm_list[-1][1]) * 100)
    else:                                
        bm_rank = sorted([r[0] for r in bm_list if r[1] < anlz_amount])[-1]        
    return bm_rank

def benchmark_ratio_rank(i_list, clinician_audit, date_to_period, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']
        pd_str = date_to_period[x[1]]
        anlz_f = clinician_audit[x[0]]['anlz_f_' + pd_str]
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)
        elif math.isnan(x[3]) == True:
            bm_rank.append(None)                    
        elif x[3] == 0:
            bm_rank.append(None)
        elif anlz_f != None:
            anlz_num = x[2] * float(anlz_f)
            anlz_denom = x[3] * float(anlz_f)
            anlz_amount = anlz_num / anlz_denom                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank

def benchmark_rank_ytd(i_list, clinician_audit, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']        
        anlz_f = clinician_audit[x[0]]['anlz_f_ytd']
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)                    
        elif anlz_f != None:
            anlz_amount = x[1] * float(anlz_f)                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank

def benchmark_ratio_rank_ytd(i_list, clinician_audit, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']        
        anlz_f = clinician_audit[x[0]]['anlz_f_ytd']
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)
        elif math.isnan(x[2]) == True:
            bm_rank.append(None)
        elif x[2] == 0:
            bm_rank.append(None)                  
        elif anlz_f != None:
            anlz_num = x[1] * float(anlz_f)
            anlz_denom = x[2] * float(anlz_f)
            anlz_amount = anlz_num / anlz_denom                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank

def benchmark_rank_trend(i_list, clinician_audit, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']        
        anlz_f = clinician_audit[x[0]]['anlz_f_trend']
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)                    
        elif anlz_f != None:
            anlz_amount = x[1] * float(anlz_f)                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank

def benchmark_ratio_rank_trend(i_list, clinician_audit, bm_dict):
    bm_rank = []    
    for x in i_list:
        specialty = clinician_audit[x[0]]['specialty']        
        anlz_f = clinician_audit[x[0]]['anlz_f_trend']
        bm_list = bm_dict[specialty]        
        if len(bm_list) == 0:                                    
            bm_rank.append(None)
        elif math.isnan(x[2]) == True:
            bm_rank.append(None)
        elif x[2] == 0:
            bm_rank.append(None)                  
        elif anlz_f != None:
            anlz_num = x[1] * float(anlz_f)
            anlz_denom = x[2] * float(anlz_f)
            anlz_amount = anlz_num / anlz_denom                        
            bm_rank.append(rank_calc(anlz_amount, bm_list))            
        else:
            bm_rank.append(None)         
    return bm_rank