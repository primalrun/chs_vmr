from datetime import datetime
from calendar import monthrange
from dateutil.relativedelta import relativedelta

def get_date_logic(default_date):
    if default_date is not None:
        cmd1_s = default_date
    else:
        cmd1_s = input('Report Month Day 1? (m/d/yyyy):')
    
    cm_s = datetime.strptime(cmd1_s, '%m/%d/%Y')
    cm_e = cm_s + relativedelta(days=monthrange(cm_s.year, cm_s.month)[1] - 1)
    cy_s = cm_s + relativedelta(months=-(cm_s.month - 1))
    pm11_s = cm_s + relativedelta(months=-11)
    date_logic_dict = {'cm_s': cm_s,
                     'cm_e': cm_e,
                     'cy_s': cy_s,
                     'pm11_s': pm11_s}
    return date_logic_dict



    
    
    
    