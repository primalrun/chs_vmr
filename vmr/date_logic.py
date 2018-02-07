from datetime import datetime
from calendar import monthrange
from dateutil.relativedelta import relativedelta
import common_functions as cf

def get_date_logic(default_date):
    if default_date is not None:
        cmd1_s = default_date
    else:
        cmd1_s = input('Report Month Day 1? (m/d/yyyy):')
    
    cm_s = datetime.strptime(cmd1_s, '%m/%d/%Y')
    cm_e = cm_s + relativedelta(days=monthrange(cm_s.year, cm_s.month)[1] - 1)
    cy_s = cm_s + relativedelta(months=-(cm_s.month - 1))
    date_logic_dict = {'cm_s': cm_s,
                     'cm_e': cm_e,
                     'cy_s': cy_s}
    dte_s = cm_s
    for x in range(1, 12):
        str_s = 'pm' + str(x) + '_s'
        dte_s = dte_s + relativedelta(months=-1)
        str_e = 'pm' + str(x) + '_e'
        dte_e = dte_s + relativedelta(
            days=monthrange(dte_s.year, dte_s.month)[1] - 1)
        date_logic_dict[str_s] = dte_s 
        date_logic_dict[str_e] = dte_e

    return date_logic_dict

def date_to_period(date_dict):
    d = {}
    for k in date_dict:
        if k.endswith('s'):
            d[date_dict[k]] = k.split('_')[0]
    return d

def work_days_possible(date_start, date_end):
    ds_s = date_start.strftime('%m/%d/%Y')
    de_s = date_end.strftime('%m/%d/%Y')
    
    sql = """
    select
       sum(w.clinicdayint) as clinic_days
    from tblworkdays w
    where
       w.clinicdate between #{d1}# and #{d2}#
    """.format(d1 = ds_s, d2 = de_s)    
    
    a = int(cf.access_retrieve_vmr(sql)[0][0])    
    return a

def work_days_actual(date_start, date_end):
    ds_s = date_start.strftime('%m/%d/%Y')
    de_s = date_end.strftime('%m/%d/%Y')
    
    sql = """
    select
       w.clinicdate,
       w.clinicdayint
    from tblworkdays w
    where
       w.clinicdate between #{d1}# and #{d2}#
    """.format(d1 = ds_s, d2 = de_s)    
    
    a = list(cf.access_retrieve_vmr(sql))
    
    return a




    
    
    
    