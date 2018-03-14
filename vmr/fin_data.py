import common_functions as cf
from collections import defaultdict
import pandas as pd



def clinician_financial(clinician_audit, clinician_coid_dept, date_dict):    
    npi_gl = []
    for k in clinician_audit:
        if clinician_audit[k]['shared_gl'] == 0:
            for e in range(0, len(clinician_coid_dept[k])):
                coid_dept = str(clinician_coid_dept[k][e]).split('.')
                npi_gl.append((k, coid_dept[0], coid_dept[1]))
    
    sql = 'drop table npi_gl'
    db_update = cf.access_update_vmr(sql)
    
    sql = """
    create table npi_gl (
       npi varchar(10),
       coid varchar(4),
       dept varchar(4)
    )
    """    
    db_update = cf.access_update_vmr(sql)

    sql = """
    insert into npi_gl (npi, coid, dept)
    values (?, ?, ?)    
    """    
    db_update = cf.access_update_many_vmr(sql, npi_gl)    
    

    #get financial data
    ds = date_dict['pm11_s'].strftime('%m/%d/%Y')
    de = date_dict['cm_e'].strftime('%m/%d/%Y')    
    sql = """
    select
       n.npi,
       e.rpt_period,
       sum(e.phcvsts) as visit,
       sum(e.[6102]) as physician_salary,
       sum(e.[6103]) as med_staff_salary,
       sum(e.[6115]) as bonus,
       sum(e.[6102] + e.[6103] + e.[6115]) as total_comp,
       sum(e.patrev) as charges,
       sum(e.tlnetrev) as collections,
       sum(e.patrev) as pat_rev,
       sum(e.revded) as rev_ded,
       sum(e.netpatrev) as net_pat_rev,
       sum(e.tlothinc) as oth_income,
       sum(e.baddebt) as bad_debt,
       sum(e.tlnetrev) as net_rev,
       sum(e.supply) as supplies,
       sum(e.medfees) as med_spec_fees,
       sum(e.salary - (e.[6102] + e.[6103] + e.[6115])) as other_salary_and_wages,
       sum(e.salary) as total_salary_and_wages,
       sum(e.benefit) as benefits,
       sum(e.clabor) as contract_labor,
       sum(e.purchsv) as purchased_services,
       sum(e.market) as marketing,
       sum(e.util) as utilities,
       sum(e.physfee) as phys_rec_fees,
       sum(e.othoper) as other_opex,
       sum(e.hitech) as hitech,
       sum(e.proptax) as prop_tax_ins,
       sum(e.repairs) as repair_and_maintenance,
       sum(e.tloprexp) as total_opex,
       sum(e.rent) as rent,
       sum(e.ebitda) as ebitda
    from essbase_actual e
       inner join npi_gl n
          on e.coid = n.coid
          and e.dept = n.dept
    where
       e.rpt_period between #{d1}# and #{d2}#
    group by
       n.npi,
       e.rpt_period
    """.format(d1 = ds, d2 = de)    
    
    df_period = cf.access_retrieve_vmr_pd(sql)
    df_trend = df_period.groupby('npi').sum()
    ds = date_dict['cy_s']
    de = date_dict['cm_e']
    df_ytd = df_period[(df_period.rpt_period >= ds) 
                       & (df_period.rpt_period <= de)]
    df_ytd = df_ytd.groupby('npi').sum()
    fin_data = {'monthly': df_period
                ,'trend_12': df_trend
                ,'ytd': df_ytd}
    
    
    return fin_data