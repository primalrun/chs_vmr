import date_logic
import coid_dept
import clinician_demo
import fin_data
import common_functions
import production
import benchmark
import pandas as pd
import pathlib
import contextlib
import os

#output file
dir_path = r'c:\\vmr\\'

pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

#default value if argument populated
#period dict, date
date_dict = date_logic.get_date_logic('12/1/2017')
 
#coid.dept dict, clinician count 
gl_clinician_count = coid_dept.coid_dept_clinician_count()

#npi dict, [coid.dept]
clinician_coid_dept = clinician_demo.clinician_coid_dept()

#npi dict, [division, top market, market, clinic, coid, employed, clinician,
#specialty, fte, category, tenure, start date, term date, provider type, 
#hospital
clinician_attribute = clinician_demo.clinician_attributes()

#npi dict, shared coid dept
clinician_shared_gl = coid_dept.clinician_shared_gl_status(
    gl_clinician_count, clinician_coid_dept)

#npi dict, dict for each element
clinician_audit = clinician_demo.clinician_audit_initial(
    clinician_attribute)

#clinician_audit add benchmark_flag, shared_gl
clinician_audit = clinician_demo.clinician_audit_benchmark(
    clinician_audit, clinician_shared_gl)

#clinician_audit add coid_dept
clinician_audit = clinician_demo.clinician_audit_coid_dept(
    clinician_audit, clinician_coid_dept)

#clinician_audit add ytd_workdays_possible, trend_12_workdays_possible
#anlz_f_ytd, anlz_f_trend, anlz_f for each month
clinician_audit = clinician_demo.clinician_audit_workdays(clinician_audit,
                                                           date_dict)
#df's--monthly, trend_12, ytd
# fin_df_dict = fin_data.clinician_financial(clinician_audit
#                                                , clinician_coid_dept
#                                                , date_dict)
#df's--monthly, trend_12, ytd
# prod_df_dict = production.clinician_production(clinician_audit, date_dict)

#dict benchmarks--wrvu, comp, coll, coll_to_wrvu, comp_to_coll
#comp_to_wrvu, wrvu_to_visit
bm_dict = benchmark.mgma_benchmark()





# file_name = dir_path + r'vmr_output.xlsx'
# with contextlib.suppress(FileNotFoundError):
#     os.remove(file_name)
# writer = pd.ExcelWriter(file_name)
# df_prod.to_excel(writer, 'monthly')
# writer.save()




# d = clinician_audit['1003007980']
# for k, v in d.items():
#     print('{}        {}'.format(k, v))











    





        
    

