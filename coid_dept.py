import common_functions as cf

def coid_dept_clinician_count():
    sql = """
    select
       cad.clinic_coid & '.' & cad.dept as coid_dept,
       count(*) as clinician_count
    from clinician_accounting_demographics cad
       inner join clinic_hierarchy ch
          on cad.clinic_coid = ch.clinic_coid
    group by
       cad.clinic_coid & '.' & cad.dept
       """
   
    lt = cf.access_retrieve_vmr(sql)
    dt = {r[0]: r[1] for r in lt}
    return dt

def clinician_shared_gl_status(coid_dept_count,
                                  clinician_coid_dept):
    d1 = {}
    for k in clinician_coid_dept:
        coid_dept = clinician_coid_dept[k]        
        multiple_count = [i for i in coid_dept
                          if coid_dept_count[i] > 1]
        if not multiple_count:
            d1[k] = 0
        else:
            d1[k] = 1
    return d1
    

    


    