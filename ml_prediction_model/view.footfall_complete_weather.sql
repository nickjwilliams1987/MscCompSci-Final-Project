WITH rolling_footfall AS (
  SELECT
  foot.city,
  foot.footfall_location,
  foot.timestamp,  
  COALESCE(
    AVG(foot.total_footfall) OVER (
      PARTITION BY foot.footfall_location, foot.city, EXTRACT(HOUR FROM timestamp) 
      ORDER BY timestamp 
      ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
    ), foot.total_footfall) as footfall_90_day_average,
  COALESCE(
    SUM(COALESCE(weather.snow,0)) OVER (
      PARTITION BY foot.footfall_location, foot.city
      ORDER BY timestamp 
      ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ), weather.snow,0) as snow_24_hour_total,
  COALESCE(
    SUM(COALESCE(weather.rain,0)) OVER (
      PARTITION BY foot.footfall_location, foot.city
      ORDER BY timestamp 
      ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ), weather.rain,0) as rain_24_hour_total,
FROM
  `footfall.footfall_complete` AS foot
INNER JOIN `weather.historic_complete` AS weather
ON foot.timestamp = weather.date
AND foot.city = weather.city
)



SELECT
  foot.city,
  foot.footfall_location,
  EXTRACT(DATE FROM foot.timestamp) as date,
  EXTRACT(YEAR FROM foot.timestamp) AS year,
  EXTRACT(MONTH FROM foot.timestamp) AS month,
  EXTRACT(WEEK FROM foot.timestamp) AS week,
  hols.name AS bank_holiday,
  sum(foot.total_footfall) as total_footfall,
  ROUND(sum(footfall_90_day_average),0) as rolling_footfall,
  ROUND(avg(weather.temp),2) as temp,
  ROUND(avg(weather.pressure),2) as pressure,
  ROUND(avg(weather.humidity),2) as humidity,
  ROUND(avg(weather.clouds),2) as clouds,
  ROUND(avg(weather.wind),2) as wind,
  ROUND(avg(COALESCE(weather.rain,0)),2) AS current_rain,
  ROUND(avg(COALESCE(roll.rain_24_hour_total,0)),2) as rain_24_hour,
  ROUND(avg(COALESCE(weather.snow,0)),2) AS current_snow,
  ROUND(avg(COALESCE(roll.snow_24_hour_total,0)),2) as snow_24_hour
FROM
  `footfall.footfall_complete` AS foot
LEFT JOIn rolling_footfall AS roll
ON foot.city = roll.city
AND foot.footfall_location = roll.footfall_location
AND foot.timestamp = roll.timestamp
LEFT JOIN
  `holidays.holidays` AS hols
ON DATE(foot.timestamp) = hols.date
INNER JOIN
  `weather.historic_complete` AS weather
  ON foot.timestamp = weather.date
  AND foot.city = weather.city
  
GROUP BY city, footfall_location, bank_holiday, date, year, month, week
