from collections import defaultdict
import common_functions as cf
import numpy as np
import pandas as pd

def mgma_benchmark():
    #wrvu
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 7
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    wrvu_bm = defaultdict(list)
    for r in result:
        wrvu_bm[r[0]].append(r[1:]) 
    
    #comp
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 3
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    comp_bm = defaultdict(list)
    for r in result:
        comp_bm[r[0]].append(r[1:])    
    
    #collection
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 1
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    coll_bm = defaultdict(list)
    for r in result:
        coll_bm[r[0]].append(r[1:])        
    
    #coll_to_wru
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 2
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    coll_to_wrvu_bm = defaultdict(list)
    for r in result:
        coll_to_wrvu_bm[r[0]].append(r[1:])
            
    #comp_to_coll
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 4
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    comp_to_coll_bm = defaultdict(list)
    for r in result:
        comp_to_coll_bm[r[0]].append(r[1:])
                    
    #comp_to_wrvu
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 5
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    comp_to_wrvu_bm = defaultdict(list)
    for r in result:
        comp_to_wrvu_bm[r[0]].append(r[1:])                    
            
    #wrvu_to_visit
    sql = """
    select
       ms.specialty,
       mb.percent_number as rank,
       mb.amount as benchmark
    from (mgma_benchmark mb
       inner join benchmark b
          on mb.benchmark_id = b.id)
       inner join mgma_specialty ms
          on mb.specialty_id = ms.id
    where
       b.id = 8
       and mb.is_current = 1    
    """
    result = cf.access_retrieve_vmr(sql)
    wrvu_to_visit_bm = defaultdict(list)
    for r in result:
        wrvu_to_visit_bm[r[0]].append(r[1:])
                    
    bm_dict = {'wrvu': wrvu_bm
               ,'comp': comp_bm
               ,'coll': coll_bm
               ,'coll_to_wrvu': coll_to_wrvu_bm
               ,'comp_to_coll': comp_to_coll_bm
               ,'comp_to_wrvu': comp_to_wrvu_bm
               ,'wrvu_to_visit': wrvu_to_visit_bm}
    
    return bm_dict


def calculate_benchmark(clinician_audit, bm_dict, prod_df_dict, 
                        fin_df_dict, date_to_period):
    #production benchmark-------------------------------------------------
    #monthly----------------
    cols= ['npi', 'rpt_period', 'wrvu', 'visit']
    df1 = prod_df_dict['monthly'][cols]      
    #only rows with wrvu values
    df1 = df1[np.isfinite(df1['wrvu'])] 
    #remove rows where clinician benchark_flag = 0
    no_bm = [k for k in clinician_audit 
             if clinician_audit[k]['benchmark_flag'] == 0]
    df1 = df1[~df1.npi.isin(no_bm)]
    df1['wrvu_to_visit'] = df1['wrvu'] / df1['visit']
    l1 = list(zip(df1.npi, df1.rpt_period, df1.wrvu))
    bm_rank = cf.benchmark_rank(
        l1, clinician_audit, date_to_period, bm_dict['wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_rank'] = se.values 
    #wrvu to visit
    l1 = list(zip(df1.npi, df1.rpt_period, df1.wrvu, df1.visit))
    bm_rank = cf.benchmark_ratio_rank(
        l1, clinician_audit, date_to_period, bm_dict['wrvu_to_visit'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_to_visit_rank'] = se.values
    df_prod_rank_monthly = df1
    
    #ytd----------------    
    df1 = prod_df_dict['ytd'].drop(['charge', 'collection'], axis=1)
    df1['npi'] = df1.index    
    cols = ['npi', 'wrvu', 'visit']
    df1 = df1[cols]    
    #only rows with wrvu values
    df1 = df1[np.isfinite(df1['wrvu'])] 
    #remove rows where clinician benchark_flag = 0
    df1 = df1[~df1.npi.isin(no_bm)]
    df1['wrvu_to_visit'] = df1.apply(
        lambda row: (row.wrvu / row.visit) if row.visit != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.wrvu))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_rank'] = se.values    
    #wrvu to visit
    l1 = list(zip(df1.npi, df1.wrvu, df1.visit))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['wrvu_to_visit'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_to_visit_rank'] = se.values
    df_prod_rank_ytd = df1    
    
    #trend----------------    
    df1 = prod_df_dict['trend_12'].drop(['charge', 'collection'], axis=1)
    df1['npi'] = df1.index    
    cols = ['npi', 'wrvu', 'visit']
    df1 = df1[cols]    
    #only rows with wrvu values
    df1 = df1[np.isfinite(df1['wrvu'])] 
    #remove rows where clinician benchark_flag = 0
    df1 = df1[~df1.npi.isin(no_bm)]
    df1['wrvu_to_visit'] = df1.apply(
        lambda row: (row.wrvu / row.visit) if row.visit != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.wrvu))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_rank'] = se.values    
    #wrvu to visit
    l1 = list(zip(df1.npi, df1.wrvu, df1.visit))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['wrvu_to_visit'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['wrvu_to_visit_rank'] = se.values
    df_prod_rank_trend = df1

    #financial benchmark-------------------------------------------------
    #monthly----------------
    cols= ['npi', 'rpt_period', 'total_comp', 'collections']
    df1 = fin_df_dict['monthly'][cols] 
    df1.columns = ['npi', 'rpt_period', 'comp', 'collection']     
    #only rows with comp and collection values
    df1 = df1[(df1['comp'].notnull()) | (df1['collection'].notnull())] 
    #remove rows where clinician benchark_flag = 0
    no_bm = [k for k in clinician_audit 
             if clinician_audit[k]['benchmark_flag'] == 0]
    df1 = df1[~df1.npi.isin(no_bm)]    
    df1['comp_to_coll'] = df1.apply(
        lambda row: (row.comp / row.collection) 
        if row.collection != 0 else None, axis=1)    
    #comp
    l1 = list(zip(df1.npi, df1.rpt_period, df1.comp))
    bm_rank = cf.benchmark_rank(
        l1, clinician_audit, date_to_period, bm_dict['comp'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_rank'] = se.values
    #coll
    l1 = list(zip(df1.npi, df1.rpt_period, df1.collection))
    bm_rank = cf.benchmark_rank(
        l1, clinician_audit, date_to_period, bm_dict['coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_rank'] = se.values    
    #comp_to_coll
    l1 = list(zip(df1.npi, df1.rpt_period, df1.comp, df1.collection))
    bm_rank = cf.benchmark_ratio_rank(
        l1, clinician_audit, date_to_period, bm_dict['comp_to_coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_coll_rank'] = se.values
    df_fin_rank_monthly = df1
        
    #ytd----------------    
    cols= ['npi', 'total_comp', 'collections']
    df1 = fin_df_dict['ytd']
    df1['npi'] = df1.index
    df1 = df1[cols] 
    df1.columns = ['npi', 'comp', 'collection']     
    #only rows with comp and collection values
    df1 = df1[(df1['comp'].notnull()) | (df1['collection'].notnull())] 
    #remove rows where clinician benchark_flag = 0
    no_bm = [k for k in clinician_audit 
             if clinician_audit[k]['benchmark_flag'] == 0]
    df1 = df1[~df1.npi.isin(no_bm)]    
    df1['comp_to_coll'] = df1.apply(
        lambda row: (row.comp / row.collection) 
        if row.collection != 0 else None, axis=1)    
    #comp
    l1 = list(zip(df1.npi, df1.comp))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['comp'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_rank'] = se.values
    #coll
    l1 = list(zip(df1.npi, df1.collection))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_rank'] = se.values    
    #comp_to_coll
    l1 = list(zip(df1.npi, df1.comp, df1.collection))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['comp_to_coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_coll_rank'] = se.values
    df_fin_rank_ytd = df1        
        
    #trend----------------    
    cols= ['npi', 'total_comp', 'collections']
    df1 = fin_df_dict['trend_12']
    df1['npi'] = df1.index
    df1 = df1[cols] 
    df1.columns = ['npi', 'comp', 'collection']     
    #only rows with comp and collection values
    df1 = df1[(df1['comp'].notnull()) | (df1['collection'].notnull())] 
    #remove rows where clinician benchark_flag = 0
    no_bm = [k for k in clinician_audit 
             if clinician_audit[k]['benchmark_flag'] == 0]
    df1 = df1[~df1.npi.isin(no_bm)]    
    df1['comp_to_coll'] = df1.apply(
        lambda row: (row.comp / row.collection) 
        if row.collection != 0 else None, axis=1)    
    #comp
    l1 = list(zip(df1.npi, df1.comp))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['comp'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_rank'] = se.values
    #coll
    l1 = list(zip(df1.npi, df1.collection))
    bm_rank = cf.benchmark_rank_ytd(
        l1, clinician_audit, bm_dict['coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_rank'] = se.values    
    #comp_to_coll
    l1 = list(zip(df1.npi, df1.comp, df1.collection))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['comp_to_coll'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_coll_rank'] = se.values
    df_fin_rank_trend = df1

    #merge fin and prod benchmark
    #monthly
    df_fin_rank_monthly.set_index(['npi', 'rpt_period'])
    df_prod_rank_monthly.set_index(['npi', 'rpt_period'])
    df1 = df_fin_rank_monthly.merge(
        df_prod_rank_monthly, on=['npi', 'rpt_period'], how='outer')    
    df1 = df1[['npi', 'rpt_period', 'comp', 'collection', 'wrvu']]
    #only rows with wrvu values
    df1 = df1[(df1['wrvu'].notnull()) | (df1['wrvu'] != 0)]
    #comp_to_wrvu
    df1['comp_to_wrvu'] = df1.apply(
        lambda row: (row.comp / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.rpt_period, df1.comp, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank(
        l1, clinician_audit, date_to_period, bm_dict['comp_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_wrvu_rank'] = se.values
    #coll_to_wrvu
    df1['coll_to_wrvu'] = df1.apply(
        lambda row: (row.collection / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.rpt_period, df1.collection, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank(
        l1, clinician_audit, date_to_period, bm_dict['coll_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_to_wrvu_rank'] = se.values
    df_fin_prod_rank_monthly = df1
                
    #ytd
    df_fin_rank_ytd.set_index(['npi'])
    df_prod_rank_ytd.set_index(['npi'])
    df1 = df_fin_rank_ytd.merge(
        df_prod_rank_ytd, on=['npi'], how='outer')    
    df1 = df1[['npi', 'comp', 'collection', 'wrvu']]
    #only rows with wrvu values
    df1 = df1[(df1['wrvu'].notnull()) | (df1['wrvu'] != 0)]
    #comp_to_wrvu
    df1['comp_to_wrvu'] = df1.apply(
        lambda row: (row.comp / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.comp, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['comp_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_wrvu_rank'] = se.values
    #coll_to_wrvu
    df1['coll_to_wrvu'] = df1.apply(
        lambda row: (row.collection / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.collection, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['coll_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_to_wrvu_rank'] = se.values
    df_fin_prod_rank_ytd = df1                
                
    #trend
    df_fin_rank_trend.set_index(['npi'])
    df_prod_rank_trend.set_index(['npi'])
    df1 = df_fin_rank_trend.merge(
        df_prod_rank_trend, on=['npi'], how='outer')    
    df1 = df1[['npi', 'comp', 'collection', 'wrvu']]
    #only rows with wrvu values
    df1 = df1[(df1['wrvu'].notnull()) | (df1['wrvu'] != 0)]
    #comp_to_wrvu
    df1['comp_to_wrvu'] = df1.apply(
        lambda row: (row.comp / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.comp, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['comp_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['comp_to_wrvu_rank'] = se.values
    #coll_to_wrvu
    df1['coll_to_wrvu'] = df1.apply(
        lambda row: (row.collection / row.wrvu) 
        if row.wrvu != 0 else None, axis=1)
    l1 = list(zip(df1.npi, df1.collection, df1.wrvu))
    bm_rank = cf.benchmark_ratio_rank_ytd(
        l1, clinician_audit, bm_dict['coll_to_wrvu'])
    #make series from list
    se = pd.Series(bm_rank)
    df1['coll_to_wrvu_rank'] = se.values
    df_fin_prod_rank_trend = df1                
                    
    benchmark_df_dict = {'prod_rank_monthly': df_prod_rank_monthly,
                         'prod_rank_ytd': df_prod_rank_ytd,
                         'prod_rank_trend': df_prod_rank_trend,
                         'fin_rank_monthly': df_fin_rank_monthly,
                         'fin_rank_ytd': df_fin_rank_ytd,
                         'fin_rank_trend': df_fin_rank_trend,
                         'fin_prod_rank_monthly': df_fin_prod_rank_monthly,
                         'fin_prod_rank_ytd': df_fin_prod_rank_ytd,
                         'fin_prod_rank_trend': df_fin_prod_rank_trend}
            
    return benchmark_df_dict








