-- Databricks notebook source
--order items observation dataset
describe orders_items_info

-- COMMAND ----------

describe store_info_1_csv

-- COMMAND ----------

--order example 
describe orders_info

-- COMMAND ----------

--ranking lojas WL pedidos com obs NO ITEM final 
select 
obs.id_loja,
ops.nome_rede,
ops.nome_loja,
ops.negocio,
ops.tipo_negocio as plataforma,
obs.qtd_pedidos,
obs.qtd_pedidos_com_obs as qtd_pedidos_com_obs_item,
obs.avg_obs_por_pedido,
ROUND(((SUM(qtd_pedidos_com_obs)/SUM(qtd_pedidos))*100),1) as percentual_pedidos_com_obs_item
from 
store_info_1_csv ops 
inner join 

(

select 
id_loja,
COUNT(distinct id_pedido) as qtd_pedidos,
SUM(case when count_obs >1 then 1 end) qtd_pedidos_com_obs,
ROUND(AVG(count_obs),1) avg_obs_por_pedido

from
(
select distinct a.id_loja,
b.id_pedido ,
count( b.observacao)  over (partition by b.id_pedido ) count_obs 

from orders_info a
inner join orders_items_info b 
on a.id_pedido = b.id_pedido 
where a.dt_pedido >='2022-09-18' 
and a.status ='FIN' 


group by b.id_pedido, b.observacao, a.id_loja
)

group by id_loja 
order by qtd_pedidos_com_obs desc 

) obs 

on ops.id_loja = obs.id_loja 

group by 
obs.id_loja,
ops.nome_rede,
ops.nome_loja,
ops.negocio,
ops.tipo_negocio, 
obs.qtd_pedidos,
obs.qtd_pedidos_com_obs,
obs.avg_obs_por_pedido

order by percentual_pedidos_com_obs_item desc 
