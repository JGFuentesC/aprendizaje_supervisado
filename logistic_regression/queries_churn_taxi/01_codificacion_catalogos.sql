CREATE OR REPLACE TABLE
  churn_taxi.viajes_reducida AS
WITH
  catTaxis AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY taxi_id) AS id_taxi
  FROM (
    SELECT
      DISTINCT taxi_id
    FROM
      `anahuac-bi.churn_taxi.viajes`) ),
  catMP AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY payment_type) AS d_medio_pago
  FROM (
    SELECT
      DISTINCT payment_type
    FROM
      `anahuac-bi.churn_taxi.viajes`) ),
  catEmpresas AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY company) AS d_empresa
  FROM (
    SELECT
      DISTINCT company
    FROM
      `anahuac-bi.churn_taxi.viajes`) )
SELECT
  * EXCEPT(taxi_id,
    payment_type,
    company)
FROM
  `anahuac-bi.churn_taxi.viajes`
INNER JOIN
  catTaxis
USING
  (taxi_id)
INNER JOIN
  catMP
USING
  (payment_type)
INNER JOIN
  catEmpresas
USING
  (company)