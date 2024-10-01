select
d.nome,
m.ano,
m.localizacao,
m.rede,
pib.pib,
pib.va,
pib.impostos_liquidos,
m.atu_ei,
m.atu_ef,
m.atu_em,
m.had_em,
m.tdi_em,
m.taxa_aprovacao_ef,
m.taxa_reprovacao_ef,
m.taxa_abandono_ef,
m.taxa_aprovacao_em,
m.taxa_abandono_em,
m.taxa_reprovacao_em,
m.dsu_ei,
m.dsu_ef,
m.dsu_em

from basedosdados.br_inep_indicadores_educacionais.municipio m
LEFT JOIN (SELECT DISTINCT id_municipio,nome,sigla_uf  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS d
    ON d.id_municipio = m.id_municipio
left join basedosdados.br_ibge_pib.municipio pib on pib.id_municipio = m.id_municipio and pib.ano = m.ano
where m.ano >= 2015
and sigla_uf =  'RJ'
