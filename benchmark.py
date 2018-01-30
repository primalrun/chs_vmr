from collections import defaultdict
import common_functions as cf

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