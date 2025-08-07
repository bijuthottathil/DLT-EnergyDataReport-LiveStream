

CREATE MATERIALIZED VIEW `power-catalog`.goldschema.vwgold_daily_customer_kwh_mv AS
SELECT
c.customer_id,
  c.first_name,
  c.last_name,
  c.city,
  c.state,
  p.plan_id,
  p.plan_name,
  p.rate_per_kwh,
  s.metername,
  DATE_TRUNC('day', s.reading_timestamp) AS reading_date,
  SUM(s.kwh_reading) AS daily_kwh_usage,
  COUNT(s.kwh_reading) AS daily_reading_count
FROM
  `power-catalog`.silverschema.silver_enriched_meter_readings s
INNER JOIN
  `power-catalog`.silverschema.silver_customers c ON s.customer_id = c.customer_id
INNER JOIN
  `power-catalog`.silverschema.silver_plans p ON s.plan_id = p.plan_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name,
  c.city,
  c.state,
  p.plan_id,
  p.plan_name,
  p.rate_per_kwh,
  s.metername,
  DATE_TRUNC('day', s.reading_timestamp);

