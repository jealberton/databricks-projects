-- Databricks notebook source
select * from status_daily_stamp

-- COMMAND ----------

select * from store_info_2

-- COMMAND ----------

--- Identificar status atual das lojas -- 

WITH BASE_STATUS AS 
(

SELECT frn_id,
restaurant_status as status_atual,
inicio_status 

FROM 
(
 
select frn_id, 
restaurant_status,
inicio_status,
ROW_NUMBER() OVER (PARTITION BY frn_id ORDER BY inicio_status DESC) as row_numb 
from

(
select frn_id,
restaurant_status,
MAX(data_inicio_status) as inicio_status
from 

(select frn_id,
restaurant_status,
status_anterior_igual,
--logica para pegar o ultimo status--
CASE
WHEN status_anterior_igual ='nao' and (LAG(status_anterior_igual) over (partition by frn_id order by reference_date))  ='sim' then reference_date 
WHEN status_anterior_igual ='sim' and (LAG(status_anterior_igual) over (partition by frn_id order by reference_date))  ='nao' then reference_date
WHEN status_anterior_igual ='nao' and (LAG(status_anterior_igual) over (partition by frn_id order by reference_date))  ='nao' then reference_date
END
data_inicio_status

from 

( select  account.frn_id,
log.restaurant_status,
log.reference_date,
CASE 
WHEN
log.restaurant_status = ( LAG(log.restaurant_status) over (partition by account.frn_id order by log.reference_date) ) THEN 'sim' ELSE 'nao' 
END status_anterior_igual

from status_daily_stamp log 
left join store_info_2 account
on log.frn_id =  account.frn_id 
where account.frn_id is not null 

group by 
log.restaurant_status,
log.reference_date,
account.frn_id

ORDER BY reference_date desc 
)


  
group by 
frn_id, 
status_anterior_igual,
restaurant_status,
reference_date

order by  frn_id, reference_date desc
)

where data_inicio_status is not null 

group by 
frn_id ,
restaurant_status
order by inicio_status desc 

)
group by 
frn_id, 
restaurant_status,
inicio_status
order by inicio_status desc

)

where row_numb =1 

GROUP BY 
frn_id,
restaurant_status ,
inicio_status

)   

select 
sf.trading_name,
sf.merchant_type,
sf.rating,
sf.city,
sf.state_label,
DATE(sf.activation_date) as activation_date,
bs.status_atual,
bs.inicio_status as dt_inicio_status_atual

from 
store_info_2 sf  
inner join 
BASE_STATUS bs 
on sf.frn_id = bs.frn_id

where sf.merchant_type in ('MARKET', 'PET', 'PHARMACY','HIPERMARKET')
and sf.activation_date is not null 

-- COMMAND ----------

--check 
select * from status_daily_stamp
where frn_id = 1686221 
order by reference_date desc 
