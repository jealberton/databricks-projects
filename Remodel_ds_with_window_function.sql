-- Databricks notebook source
describe stores_info_3

-- COMMAND ----------

Select 
CaseId, 
Field,
OldValue,
NewValue,
CreatedDate from status_history_2 order by CreatedDate

-- COMMAND ----------

SELECT 
  sla_imp.case_id, 
  case_hist.OldValue step,
  CASE
    WHEN case_hist.OldValue = 'Kickoff meeting' THEN sla_imp.case_created_date
    WHEN case_hist.OldValue ='Integration' AND (case_hist.NewValue = 'Registration' or case_hist.NewValue ='Cancelado') THEN LAG(case_hist.CreatedDate) OVER (PARTITION BY sla_imp.case_number ORDER BY case_hist.CreatedDate)
    WHEN case_hist.OldValue ='Registration' AND (case_hist.NewValue = 'Training' or case_hist.NewValue ='Cancelado')THEN LAG(case_hist.CreatedDate) OVER (PARTITION BY sla_imp.case_number ORDER BY case_hist.CreatedDate)
    WHEN case_hist.OldValue ='Training' AND (case_hist.NewValue = 'Activation' or case_hist.NewValue ='Cancelado') THEN LAG(case_hist.CreatedDate) OVER (PARTITION BY sla_imp.case_number ORDER BY case_hist.CreatedDate)
    WHEN case_hist.OldValue ='Activation' AND (case_hist.NewValue ='Encerrado' or case_hist.NewValue ='Cancelado') THEN LAG(case_hist.CreatedDate) OVER (PARTITION BY sla_imp.case_number ORDER BY case_hist.CreatedDate)
  END begin_step,
  case_hist.CreatedDate end_step,
  DATE(case_hist.CreatedDate) dt_partition
FROM 
  stores_info_3 AS sla_imp 
INNER JOIN 
  status_history_2 AS case_hist 
    ON sla_imp.case_id = case_hist.CaseId
