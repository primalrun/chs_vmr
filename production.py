import common_functions as cf
import pandas as pd

def clinician_production(clinician_audit, date_dict):
    npi_prod = [(k,) for k in clinician_audit]    
    sql = 'drop table npi_prod'
    db_update = cf.access_update_vmr(sql)
    
    sql = """
    create table npi_prod (
       npi varchar(10)
    )
    """    
    db_update = cf.access_update_vmr(sql)

    sql = """
    insert into npi_prod (npi)
    values (?)    
    """    
    db_update = cf.access_update_many_vmr(sql, npi_prod)    
    
    #get production data    
    ds = date_dict['pm11_s'].strftime('%m/%d/%Y')
    de = date_dict['cm_e'].strftime('%m/%d/%Y')
    
    #wrvu    
    sql = """
    select
       ad.npi,
       f.post_period as rpt_period,
       sum(f.cms_adj_wrvu) as wrvu
    from (((cms_adj_wrvu_month f
       inner join tblGLToAthena gta
          on f.tablespace = gta.tablespace
          and f.rendering_provider = gta.renderingprovider)
       inner join athena_dim_data ad
          on f.tablespace = ad.tablespace
          and f.rendering_provider = ad.rendering_provider)
       inner join clinician_accounting_demographics cad
          on ad.npi = cad.npi)
       inner join npi_prod p
          on cad.npi = p.npi 
    where
       f.post_period between #{}# and #{}#
    group by
       ad.npi,
       f.post_period
    having    
       abs(sum(f.cms_adj_wrvu))>0
    """.format(ds, de)
    df_wrvu = cf.access_retrieve_vmr_pd(sql)
    
    #visit
    sql = """
    select
       ad.npi,
       f.post_period as rpt_period,
       sum(f.pat_encounter) as visit
    from (((patient_encounter_month f
       inner join tblGLToAthena gta
          on f.tablespace = gta.tablespace
          and f.rendering_provider = gta.renderingprovider)
       inner join athena_dim_data ad
          on f.tablespace = ad.tablespace
          and f.rendering_provider = ad.rendering_provider)
       inner join clinician_accounting_demographics cad
          on ad.npi = cad.npi)
       inner join npi_prod p
          on cad.npi = p.npi 
    where
       f.post_period between #{}# and #{}#
    group by
       ad.npi,
       f.post_period
    having    
       abs(sum(f.pat_encounter))>0
    """.format(ds, de)    
    df_visit = cf.access_retrieve_vmr_pd(sql)
    
    #charge, collection
    sql = """
    select
       ad.npi,
       f.post_period as rpt_period,
       sum(f.charge) as charge,
       sum(f.net_payment) as collection
    from (((charge_payment_adjustment f
       inner join tblGLToAthena gta
          on f.tablespace = gta.tablespace
          and f.rendering_provider = gta.renderingprovider)
       inner join athena_dim_data ad
          on f.tablespace = ad.tablespace
          and f.rendering_provider = ad.rendering_provider)
       inner join clinician_accounting_demographics cad
          on ad.npi = cad.npi)
       inner join npi_prod p
          on cad.npi = p.npi 
    where
       f.post_period between #{}# and #{}#
    group by
       ad.npi,
       f.post_period
    having    
       (abs(sum(f.charge))>0 or abs(sum(f.net_payment))>0)
    """.format(ds, de)    
    df_charge_coll = cf.access_retrieve_vmr_pd(sql)    
    
    #combine production dataframes    
    df_wrvu.set_index(['npi', 'rpt_period'])
    df_visit.set_index(['npi', 'rpt_period'])
    df_charge_coll.set_index(['npi', 'rpt_period'])
    df_period = df_wrvu.merge(
        df_visit, on=['npi', 'rpt_period'], how='outer').merge(
            df_charge_coll, on=['npi', 'rpt_period'], how='outer')

    df_trend = df_period.groupby('npi').sum()
    ds = date_dict['cy_s']
    de = date_dict['cm_e']
    df_ytd = df_period[(df_period.rpt_period >= ds) 
                       & (df_period.rpt_period <= de)]
    df_ytd = df_ytd.groupby('npi').sum()    
    prod_data = {'monthly': df_period,
                 'trend_12': df_trend,
                 'ytd': df_ytd}
    
    
    return prod_data
