import date_logic
import coid_dept
import clinician_demo
import fin_data
import common_functions
import production
import benchmark
import sys
import pandas as pd
import pathlib
import contextlib
import os

#output file
dir_path = r'c:\\vmr\\'

pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

#default value if argument populated
#period dict, date
date_dict = date_logic.get_date_logic('2/1/2018')

#date dict to get period
date_to_period = date_logic.date_to_period(date_dict)

#coid.dept dict, clinician count 
gl_clinician_count = coid_dept.coid_dept_clinician_count()

#npi dict, [coid.dept]
clinician_coid_dept = clinician_demo.clinician_coid_dept()

#npi dict, [division, top market, market, state, clinic, coid, employed, clinician,
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
fin_df_dict = fin_data.clinician_financial(clinician_audit
                                                , clinician_coid_dept
                                                , date_dict)
#df's--monthly, trend_12, ytd
prod_df_dict = production.clinician_production(clinician_audit, date_dict)
# print(prod_df_dict['ytd'])
# sys.exit()

#dict benchmarks--wrvu, comp, coll, coll_to_wrvu, comp_to_coll
#comp_to_wrvu, wrvu_to_visit
bm_dict = benchmark.mgma_benchmark()

rank_dict = benchmark.calculate_benchmark(clinician_audit
                                          ,bm_dict
                                          ,prod_df_dict
                                          ,fin_df_dict
                                          ,date_to_period)

df_fin_monthly = fin_df_dict['monthly']
df_fin_ytd = fin_df_dict['ytd']
df_fin_trend = fin_df_dict['trend_12']
df_prod_monthly = prod_df_dict['monthly']
df_prod_ytd = prod_df_dict['ytd']
df_prod_trend = prod_df_dict['trend_12']
df_prod_rank_monthly = rank_dict['prod_rank_monthly']
df_prod_rank_ytd = rank_dict['prod_rank_ytd']
df_prod_rank_trend = rank_dict['prod_rank_trend']
df_fin_rank_monthly = rank_dict['fin_rank_monthly']
df_fin_rank_ytd = rank_dict['fin_rank_ytd']
df_fin_rank_trend = rank_dict['fin_rank_trend']
df_fin_prod_rank_monthly = rank_dict['fin_prod_rank_monthly']
df_fin_prod_rank_ytd = rank_dict['fin_prod_rank_ytd']
df_fin_prod_rank_trend = rank_dict['fin_prod_rank_trend']
df_ca = pd.DataFrame.from_dict(clinician_audit, orient='index')
df_ca.index.name = 'npi'
df_period = pd.DataFrame.from_dict(date_dict, orient='index')
df_period.index.name = 'period'
df_period.columns = ['period_date']

#output.xlsx
file_output = dir_path + r'vmr_output.xlsx'
with contextlib.suppress(FileNotFoundError):
    os.remove(file_output)
writer = pd.ExcelWriter(file_output)
df_fin_monthly.to_excel(writer, 'fin_mth', index=False)
df_fin_ytd.to_excel(writer, 'fin_ytd', index=True)
df_fin_trend.to_excel(writer, 'fin_trend', index=True)
df_prod_monthly.to_excel(writer, 'prod_mth', index=False)
df_prod_ytd.to_excel(writer, 'prod_ytd', index=True)
df_prod_trend.to_excel(writer, 'prod_trend', index=True)
df_prod_rank_monthly.to_excel(writer, 'prod_rank_mth', index=False)
df_prod_rank_ytd.to_excel(writer, 'prod_rank_ytd', index=False)
df_prod_rank_trend.to_excel(writer, 'prod_rank_trend', index=False)
df_fin_rank_monthly.to_excel(writer, 'fin_rank_mth', index=False)
df_fin_rank_ytd.to_excel(writer, 'fin_rank_ytd', index=False)
df_fin_rank_trend.to_excel(writer, 'fin_rank_trend', index=False)
df_fin_prod_rank_monthly.to_excel(writer, 'fin_prod_rank_mth', index=False)
df_fin_prod_rank_ytd.to_excel(writer, 'fin_prod_rank_ytd', index=False)
df_fin_prod_rank_trend.to_excel(writer, 'fin_prod_rank_trend', index=False)
df_ca.to_excel(writer, 'clinician')
df_period.to_excel(writer, 'period')
writer.save()



print('Done')


 

# d = clinician_audit['1023229895']
# for k, v in d.items():
#     print('{}        {}'.format(k, v))











    





        
    

