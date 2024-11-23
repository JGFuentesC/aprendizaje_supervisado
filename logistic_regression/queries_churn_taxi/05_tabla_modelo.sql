SELECT
  * EXCEPT(d_mes),
  EXTRACT(month
  FROM
    fh_mes) d_mes
FROM
  `anahuac-bi.churn_taxi.tad`
WHERE
  c_num_meses_activo>=4