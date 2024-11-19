CREATE OR REPLACE TABLE
  churn_taxi.agg_cat AS
SELECT
  id_taxi,
  fh_mes,
  CASE
    WHEN d_empresa IN (167, 137, 135, 176, 117, 164, 122, 133, 131, 144, 125, 166, 139) THEN CONCAT('E',CAST(d_empresa AS string))
    ELSE 'OTRA'
END
  AS d_empresa,
  CASE
    WHEN d_medio_pago<=2 THEN CONCAT('MP',CAST(d_medio_pago AS string))
    ELSE 'OTRO'
END
  AS d_medio_pago,
  COUNT(*) AS c_num_viajes,
  SUM(c_distancia) AS c_distancia,
  SUM(c_duracion) AS c_duracion,
  SUM(c_propinas) AS c_propinas,
  SUM(c_total_viaje) AS c_total_viaje
FROM
  `anahuac-bi.churn_taxi.viajes_reducida`
GROUP BY
  all