import date_logic
import coid_dept
import clinician_demo

#default value if argument populated
#period dict, date
dict_date_logic = date_logic.get_date_logic('12/1/2017')
 
#coid.dept dict, clinician count 
gl_clinician_count = coid_dept.fx_coid_dept_clinician_count()

#npi dict, [coid.dept]
clinician_coid_dept = clinician_demo.fx_clinician_coid_dept()

#npi dict, [division, top market, market, clinic, coid, employed, clinician,
#specialty, fte, category, tenure, start date, term date, provider type, 
#hospital
clinician_attribute = clinician_demo.fx_clinician_attributes()

#npi dict, shared coid dept
clinician_shared_gl = coid_dept.fx_clinician_shared_gl_status(
    gl_clinician_count, clinician_coid_dept)

#npi dict, dict for each element
clinician_audit = clinician_demo.fx_clinician_audit_initial(
    clinician_attribute)

#clinician_audit add benchmark_flag
clinician_audit = clinician_demo.fx_clinician_audit_benchmark(
    clinician_audit)

#clinician_audit add coid_dept
clinician_audit = clinician_demo.fx_clinician_audit_coid_dept(
    clinician_audit, clinician_coid_dept)


