import common_functions as cf
import date_logic



def clinician_coid_dept():
    sql = """
    select distinct
       cad.npi,
       cad.clinic_coid & '.' & cad.dept as coid_dept
    from clinician_accounting_demographics cad
       inner join clinic_hierarchy ch
          on cad.clinic_coid = ch.clinic_coid
    where
       ch.divested_flag = 'N'
    """
    
    lt = cf.access_retrieve_vmr(sql)
    dt = dict()
    
    for r in lt:
        dt.setdefault(r[0], []).append(r[1])
    
    return dt

def clinician_attributes():
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
    where
       ch.divested_flag = 'N'          
    """
    
    lt = cf.access_retrieve_vmr(sql)
    dt = {r[0]: r[1:] for r in lt}
    return dt
       
def clinician_audit_initial(clinician_attribute):
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

def clinician_audit_benchmark(clinician_audit, clinician_shared_gl):
    for k in clinician_audit:
        if (clinician_audit[k]['fte'] > 0
            and clinician_audit[k]['start_date'] != None
            and clinician_audit[k]['term_date'] == None):
                clinician_audit[k]['benchmark_flag']= 1
        else:
                clinician_audit[k]['benchmark_flag']= 0
        clinician_audit[k]['shared_gl']= clinician_shared_gl[k]
    return clinician_audit
            

def clinician_audit_coid_dept(clinician_audit, clinician_coid_dept):
    for k in clinician_audit:
        coid_dept = clinician_coid_dept[k]
        s1 = ','.join(coid_dept)
        clinician_audit[k]['coid_dept'] = s1
    return clinician_audit                     


def clinician_audit_workdays(clinician_audit, date_dict):
    ytd_work_days_possible = date_logic.work_days_possible(
        date_dict['cy_s'],
        date_dict['cm_e'])    
    trend_12_work_days_possible = date_logic.work_days_possible(
        date_dict['pm11_s'],
        date_dict['cm_e'])    
    

    
    #list [date, work day value]  
    ytd_work_days = date_logic.work_days_actual(date_dict['cy_s'],
                                            date_dict['cm_e'])
    
    trend_work_days = date_logic.work_days_actual(date_dict['pm11_s'],
                                            date_dict['cm_e'])
        
    for k in clinician_audit:
        clinician_audit[k]['ytd_workdays_possible'] = (
            ytd_work_days_possible)
        clinician_audit[k]['trend_12_workdays_possible'] = (
            trend_12_work_days_possible)            
        if clinician_audit[k]['benchmark_flag'] == 1: 
            start_date = clinician_audit[k]['start_date']
            fte = clinician_audit[k]['fte']

            wd_ytd = sum(x[1] for x in ytd_work_days if x[0] >= start_date)
            if wd_ytd > 0:
                wd_ytd_fte_adj = wd_ytd * fte
                anlz_f_ytd = (ytd_work_days_possible / wd_ytd_fte_adj)
                clinician_audit[k]['anlz_f_ytd'] = anlz_f_ytd
                clinician_audit[k]['wd_fte_adj_ytd'] = wd_ytd_fte_adj
            else:
                clinician_audit[k]['anlz_f_ytd'] = None
                clinician_audit[k]['wd_fte_adj_ytd'] = None

            wd_trend = sum(x[1] for x in trend_work_days if x[0] >= start_date)
            if wd_trend > 0:
                wd_trend_fte_adj = wd_trend * fte
                anlz_f_trend = (trend_12_work_days_possible / wd_trend_fte_adj)
                clinician_audit[k]['anlz_f_trend'] = anlz_f_trend
                clinician_audit[k]['wd_fte_adj_trend'] = wd_trend_fte_adj                
            else:
                clinician_audit[k]['anlz_f_trend'] = None
                clinician_audit[k]['wd_fte_adj_trend'] = None
            #cm
            dt_s = date_dict['cm_s']
            dt_e = date_dict['cm_e']
            wd = sum(x[1] for x in trend_work_days if (x[0] >= start_date
                        and dt_s <= x[0] <= dt_e))
            if wd > 0:
                wd_fte_adj = wd * fte
                anlz_f = (trend_12_work_days_possible / wd_fte_adj)
                clinician_audit[k]['anlz_f_cm'] = anlz_f
                clinician_audit[k]['wd_fte_adj_cm'] = wd_fte_adj
            else:
                clinician_audit[k]['anlz_f_cm'] = None
                clinician_audit[k]['wd_fte_adj_cm'] = None
            
            #prior months
            for x in range(1, 12):
                dt_s = date_dict['pm' + str(x) +'_s']
                dt_e = date_dict['pm' + str(x) +'_e']
                wd = sum(x[1] for x in trend_work_days if (x[0] >= start_date
                            and dt_s <= x[0] <= dt_e))
                if wd > 0:
                    wd_fte_adj = wd * fte
                    anlz_f = (trend_12_work_days_possible / wd_fte_adj)
                    clinician_audit[k]['anlz_f_pm' + str(x)] = anlz_f
                    clinician_audit[k]['wd_fte_adj_pm' + str(x)] = wd_fte_adj            
                else:
                    clinician_audit[k]['anlz_f_pm' + str(x)] = None
                    clinician_audit[k]['wd_fte_adj_pm' + str(x)] = None
            

        else:
            clinician_audit[k]['ytd_workdays_possible'] = None
            clinician_audit[k]['trend_12_workdays_possible'] = None
            clinician_audit[k]['wd_fte_adj_ytd'] = None
            clinician_audit[k]['anlz_f_ytd'] = None
            clinician_audit[k]['wd_fte_adj_trend'] = None
            clinician_audit[k]['anlz_f_trend'] = None
            clinician_audit[k]['wd_fte_adj_cm'] = None
            clinician_audit[k]['anlz_f_cm'] = None
            for x in range(1, 12):
                clinician_audit[k]['wd_fte_adj_pm' + str(x)] = None
                clinician_audit[k]['anlz_f_pm' + str(x)] = None            
            
    return clinician_audit
            
        
    