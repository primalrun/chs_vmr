import common_functions as cf




def fx_clinician_coid_dept():
    sql = """
    select distinct
       cad.npi,
       cad.clinic_coid & '.' & cad.dept as coid_dept
    from clinician_accounting_demographics cad
       inner join clinic_hierarchy ch
          on cad.clinic_coid = ch.clinic_coid
    """
    
    lt = cf.access_retrieve_vmr(sql)
    dt = dict()
    
    for r in lt:
        dt.setdefault(r[0], []).append(r[1])
    
    return dt

def fx_clinician_attributes():
    sql = """
    select
       q1.npi, 
       ch.division,  
       ch.top_market_name,
       ch.market_name,
       ch.clinic_name,
       q1.coid,  
       iif(q1.source = 'roster', 'employed', 'non-employed') as employed_status,
       q1.clinician,
       q1.specialty,
       q1.fte,
       q1.category,
       q1.tenure,
       q1.start_date,
       q1.term_date,
       q1.provider_type,
       iif(left(q1.category, 1) in ('H', 'P'), 'hospital', 'not hospital') AS is_hospital
    from (
    select
       cad.npi,
       max(cad.clinic_coid) as coid,
       max(cad.source) as source,
       max(cad.clinician) as clinician,
       max(cad.specialty) as specialty,
       sum(cad.fte) as fte,
       max(cad.category) as category,
       max(cad.tenure) as tenure,
       max(cad.start_date) as start_date,
       max(cad.term_date) as term_date,
       max(cad.provider_type) as provider_type
    from clinician_accounting_demographics cad
    group by
       cad.npi
    ) q1
       inner join clinic_hierarchy ch
          on ch.clinic_coid = q1.coid
    """
    
    lt = cf.access_retrieve_vmr(sql)
    dt = {r[0]: r[1:] for r in lt}
    return dt
       
def fx_clinician_audit_initial(clinician_attribute):
    d1 = {}
    
    for k in clinician_attribute:
        d1[k] = {'division': clinician_attribute[k][0],
                 'top_market': clinician_attribute[k][1],
                 'market': clinician_attribute[k][2],
                 'clinic': clinician_attribute[k][3],
                 'coid': clinician_attribute[k][4],
                 'employed_status': clinician_attribute[k][5],
                 'clinician': clinician_attribute[k][6],
                 'specialty': clinician_attribute[k][7],
                 'fte': clinician_attribute[k][8],
                 'category': clinician_attribute[k][9],
                 'tenure': clinician_attribute[k][10],
                 'start_date': clinician_attribute[k][11],
                 'term_date': clinician_attribute[k][12],
                 'physician_flag': clinician_attribute[k][13],
                 'hospital_flag': clinician_attribute[k][14]
                 }
    
    return d1

def fx_clinician_audit_benchmark(clinician_audit):
    for k in clinician_audit:
        if (clinician_audit[k]['fte'] > 0
            and clinician_audit[k]['term_date'] == None):
                clinician_audit[k]['benchmark_flag']= 1
        else:
                clinician_audit[k]['benchmark_flag']= 0
    return clinician_audit
            

def fx_clinician_audit_coid_dept(clinician_audit, clinician_coid_dept):
    for k in clinician_audit:
        coid_dept = clinician_coid_dept[k]
        s1 = ','.join(coid_dept)
        clinician_audit[k]['coid_dept'] = s1
    return clinician_audit                     
