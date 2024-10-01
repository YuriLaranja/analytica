-- Databricks notebook source
-- MAGIC %python
-- MAGIC from ifood_databricks.toolbelt.string import normalize, unaccent
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from ifood_databricks import etl
-- MAGIC from ifood_databricks import datalake
-- MAGIC from ifood_databricks.gcp import gsheet
-- MAGIC from ifood_databricks.gcp.gsheet import gsheets_data_dump
-- MAGIC
-- MAGIC spark.udf.register("normalize", normalize)
-- MAGIC spark.udf.register("unaccent", unaccent)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = gsheet.get_dataset(
-- MAGIC     spreadsheet_id="1EhzXkgHJzawqCVMWCpeCm0jK5lE4nJ_uRajC1FUTD1o", range="base_pre!A:U"
-- MAGIC )
-- MAGIC df.createOrReplaceTempView("base_analytica")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Alunos por Ano e taxa abandono
-- MAGIC ## Filtros feitos para retirar diversos tipos de Escolas: Federais, Municipais. 
-- MAGIC ## Analise feita a partir do Total

-- COMMAND ----------

-- Alunos por ano e taxa abandono
select 
ano,
round(avg(atu_em),2) as alunos,
round(avg(taxa_abandono_em),2) as media_tx_abandono
from base_analytica
where rede = 'total' -- 
and localizacao = 'total'
group by 1
order by 1 asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Criando uma temp view que Agrupa as cidades pela media do PIB, e ordenando pela maior media, usando o limit 10 para trazer somente as 10 primeiras

-- COMMAND ----------

create or replace temp view maiores_pibs as
select 
nome,
round(avg(pib),2) as media_pib,
count(1) qtd_linhas
from base_analytica
where rede = 'total'
and localizacao = 'total'
group by 1
order by 2 desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC # Criando uma temp view que Agrupa as cidades pela media do PIB, e ordenando pela menor media, usando o limit 10 para trazer somente as 10 primeiras

-- COMMAND ----------

create or replace temp view menores_pibs as
select 
nome,
round(avg(pib),2) as media_pib,
count(1) qtd_linhas
from base_analytica
where rede = 'total'
and localizacao = 'total'
group by 1
order by 2 asc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fazendo um union entre as cidades com maiores PIB e menores PIB para fazer o gráfico de representatividade no Estado, dividindo por 1000000 para ficar mais visual 

-- COMMAND ----------

select
'Top 10 maior PIB' as info_pib,
nome,
round(media_pib/1000000,1) as media_pib
from maiores_pibs

union all
select
'Top 10 menor PIB' as info_pib,
nome,
round(media_pib/1000000,1) as media_pib
from menores_pibs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Base com joins com as views e filtro para pegar as informações dos Totais das Cidades (filtrando somente as cidades do Top 10 maiores e menores )

-- COMMAND ----------

create or replace temp view base_analise_fim as
select 
case when ma.nome is not null then 'Top 10 maior PIB'
     when me.nome is not null then 'Top 10 menor PIB'
     else 'Outros' end as info_pib,
b.*
from base_analytica b
left join maiores_pibs ma on ma.nome = b.nome
left join menores_pibs me on me.nome = b.nome
where rede = 'total'
and localizacao = 'total'
and (ma.nome is not null or me.nome is not null)

-- COMMAND ----------

select * from base_analise_fim

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Calculando a media das métricas a partir do Agrupamento criado

-- COMMAND ----------

select
info_pib,
round(avg(atu_ei),2) as media_alunos_ei,
round(avg(atu_ef),2) as media_alunos_ef,
round(avg(atu_em),2) as media_alunos_em,
round(avg(taxa_abandono_em),2) as media_tx_abandono,
round(avg(dsu_em),2) as media_professores_graduados,
sum(impostos_liquidos) as impostos_liquidos
from base_analise_fim
group by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Trazendo métricas por ano e Cidades para análises mais granulares da Top 10 maiores PIB

-- COMMAND ----------

select
ano,
nome,
round(avg(atu_ei),2) as media_alunos_ei,
round(avg(atu_ef),2) as media_alunos_ef,
round(avg(atu_em),2) as media_alunos_em,
round(avg(taxa_abandono_em),2) as media_tx_abandono,
round(avg(dsu_em),2) as media_professores_graduados
from base_analise_fim
where info_pib = 'Top 10 maior PIB'
group by 1,2
