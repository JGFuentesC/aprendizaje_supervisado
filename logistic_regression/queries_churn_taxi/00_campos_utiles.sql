  /*
Determinar la probabilidad de que un taxi específico deje de
 transaccionar en los siguientes 3 meses después de observado al 
 considerar su historia reciente (6 meses)
*/
CREATE OR REPLACE TABLE
  churn_taxi.viajes AS
SELECT
  taxi_id,
  CAST(DATE_TRUNC(trip_start_timestamp,month) AS date) fh_mes,
  trip_seconds c_duracion,
  trip_miles/1.6 c_distancia,
  trip_total c_total_viaje,
  tips c_propinas,
  payment_type,
  company
FROM
  `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE
  company IS NOT NULL
  AND trip_start_timestamp IS NOT NULL;