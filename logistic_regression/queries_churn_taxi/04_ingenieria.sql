  /*
   V1| Compensación total debajo del promedio de los ult 6 meses de todos los taxis(bool)| Alejandro,
   V2| Ticket promedio incluyendo propinas ult 6 meses (continua) | Aldo n
   V3| Número de Meses activo en el periodo (continua ) | Mauricio 
   V4| Promedio de ingreso por mes con/sin propinas (continua) | Montse 
   V5| Arriba del ticket promedio si/no (bool) | Renata
   V6| Distancia promedio por viaje (continua) | Pablo 
   V7| Compañía principal (discreta) | Prof 
   V8| % de ingreso en efectivo (continua) | Prof 
   V9| Mes del año (discreta) | Prof 
  */
CREATE OR REPLACE TABLE churn_taxi.tad as 
WITH
  catalogoTaxiMes AS (
  SELECT
    *
  FROM (
    SELECT
      DISTINCT id_taxi
    FROM
      churn_taxi.agg_cat)
  CROSS JOIN (
    SELECT
      DISTINCT fh_mes
    FROM
      churn_taxi.agg_cat) ),
  V1 AS (
  SELECT
    id_taxi,
    fh_mes,
    CASE
      WHEN c_com_total<=36800 THEN TRUE
      ELSE FALSE
  END
    AS d_debajo_prom_ing
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      c_comp,
      SUM(c_comp) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_com_total,
      SUM(CASE
          WHEN c_comp>0 THEN 1
          ELSE 0
      END
        ) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_num_meses_activo
    FROM (
      SELECT
        A.*,
        COALESCE(B.c_comp,0) c_comp
      FROM
        catalogoTaxiMes A
      LEFT JOIN (
        SELECT
          id_taxi,
          fh_mes,
          SUM(c_total_viaje+c_propinas) AS c_comp
        FROM
          churn_taxi.agg_cat
        GROUP BY
          ALL) B
      USING
        (id_taxi,
          fh_mes)))
  WHERE
    c_num_meses_activo>=4 ),
  V2 AS (
  SELECT
    id_taxi,
    fh_mes,
    SAFE_DIVIDE(c_comp,c_num_viajes) AS c_ticket_prom
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      SUM(c_comp) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_comp,
      SUM(c_num_viajes) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_num_viajes,
      SUM(CASE
          WHEN c_comp>0 THEN 1
          ELSE 0
      END
        ) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_num_meses_activo
    FROM (
      SELECT
        A.*,
        COALESCE(B.c_comp,0) c_comp,
        COALESCE(B.c_num_viajes,0) c_num_viajes
      FROM
        catalogoTaxiMes A
      LEFT JOIN (
        SELECT
          id_taxi,
          fh_mes,
          SUM(c_total_viaje+c_propinas) AS c_comp,
          SUM(c_num_viajes) AS c_num_viajes
        FROM
          churn_taxi.agg_cat
        GROUP BY
          ALL) B
      USING
        (id_taxi,
          fh_mes)))
  WHERE
    c_num_meses_activo>=4 ),
  V3 AS (
  SELECT
    id_taxi,
    fh_mes,
    c_num_meses_activo
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      SUM(CASE
          WHEN c_comp>0 THEN 1
          ELSE 0
      END
        ) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_num_meses_activo
    FROM (
      SELECT
        A.*,
        COALESCE(B.c_comp,0) c_comp,
      FROM
        catalogoTaxiMes A
      LEFT JOIN (
        SELECT
          id_taxi,
          fh_mes,
          SUM(c_total_viaje+c_propinas) AS c_comp
        FROM
          churn_taxi.agg_cat
        GROUP BY
          ALL) B
      USING
        (id_taxi,
          fh_mes)))
  WHERE
    c_num_meses_activo>=4 ),
  V4 AS (
  SELECT
    id_taxi,
    fh_mes,
    c_prom_ing_mes_c_prop,
    c_prom_ing_mes_s_prop
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      AVG(c_comp) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) c_prom_ing_mes_c_prop,
      AVG(c_total_viaje) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) c_prom_ing_mes_s_prop,
      SUM(CASE
          WHEN c_comp>0 THEN 1
          ELSE 0
      END
        ) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) AS c_num_meses_activo
    FROM (
      SELECT
        A.*,
        COALESCE(B.c_comp,0) c_comp,
        COALESCE(B.c_total_viaje,0) c_total_viaje
      FROM
        catalogoTaxiMes A
      LEFT JOIN (
        SELECT
          id_taxi,
          fh_mes,
          SUM(c_total_viaje+c_propinas) AS c_comp,
          SUM(c_total_viaje) AS c_total_viaje
        FROM
          churn_taxi.agg_cat
        GROUP BY
          ALL) B
      USING
        (id_taxi,
          fh_mes)))
  WHERE
    c_num_meses_activo>=4 ),
  V5 AS (
  SELECT
    id_taxi,
    fh_mes,
    SAFE_DIVIDE(SUM(c_viajes_malos) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING), SUM(c_num_viajes) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) ) AS c_pct_viajes_malos
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      SUM(CASE
          WHEN (c_total_viaje+c_propinas)<21.31 THEN 1
          ELSE 0
      END
        ) AS c_viajes_malos,
      COUNT(*) AS c_num_viajes
    FROM
      `anahuac-bi.churn_taxi.viajes_reducida`
    GROUP BY
      ALL) ),
V6 as (
     SELECT
    id_taxi,
    fh_mes,
    SAFE_DIVIDE(SUM(c_distancia) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING), SUM(c_num_viajes) OVER (PARTITION BY id_taxi ORDER BY fh_mes ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) ) AS c_distancia_promedio
  FROM (
    SELECT
      id_taxi,
      fh_mes,
      sum(c_distancia) as c_distancia,
      COUNT(*) AS c_num_viajes
    FROM
      `anahuac-bi.churn_taxi.viajes_reducida`
    GROUP BY
      ALL)     
),
V7 as (select id_taxi,fh_mes,d_empresa from (select *,row_number() over (partition by id_taxi,fh_mes,
d_empresa ORDER BY c_num_viajes desc) as rn from 
(select id_taxi,fh_mes,
d_empresa,sum(c_num_viajes) as c_num_viajes
from `anahuac-bi.churn_taxi.agg_cat`
group by all 
order by 1,2,3))
where rn = 1),
V8 as (select id_taxi,fh_mes, 
safe_divide(sum(case when d_medio_pago='MP1' then c_total_viaje else 0 end),
sum(c_total_viaje)) as c_pct_cash
from `anahuac-bi.churn_taxi.agg_cat`
group by all),
V9 as (
  select id_taxi,fh_mes,
  extract(month from fh_mes) as d_mes
  from `anahuac-bi.churn_taxi.agg_cat`
),
obj as (
   
    select id_taxi,fh_mes,case when sum(c_comp) over (partition by id_taxi order by fh_mes rows between 1 following and 3 following)>0 then 0 else 1 end as fuga from
  (
      SELECT
        A.*,
        COALESCE(B.c_comp,0) c_comp
      FROM
        catalogoTaxiMes A
      LEFT JOIN (
        SELECT
          id_taxi,
          fh_mes,
          SUM(c_total_viaje+c_propinas) AS c_comp
        FROM
          churn_taxi.agg_cat
        GROUP BY
          ALL) B
      USING
        (id_taxi,
          fh_mes))
)
SELECT
  *
FROM
  V1
FULL OUTER JOIN
  V2
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V3
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V4
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V5
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V6
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V7
USING
  (id_taxi,
    fh_mes)
FULL OUTER JOIN
  V8
USING
  (id_taxi,
    fh_mes)
  FULL OUTER JOIN
  V9
USING
  (id_taxi,
    fh_mes)
  FULL OUTER JOIN
  obj
USING
  (id_taxi,
    fh_mes)
ORDER BY
  id_taxi,
  fh_mes