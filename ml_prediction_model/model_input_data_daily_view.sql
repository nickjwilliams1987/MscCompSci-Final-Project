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
  #foot.timestamp,
  /* rolling_average: Get the average of the previous 90 days for the location, city and hour. If this generates an error (Coalesce), just use the actual current footfall */
  
  FORMAT_TIMESTAMP('%A',foot.timestamp) as weekday,
  FORMAT_TIMESTAMP('%b',foot.timestamp) as month,
  CASE WHEN hols.name IS NOT NULL THEN 1 ELSE 0 END AS bank_holiday,
  sum(foot.total_footfall) as total_footfall,
  ROUND(sum(footfall_90_day_average),0) as rolling_footfall,
  avg(weather.temp) as temp,
  avg(weather.pressure) as pressure,
  avg(weather.humidity) as humidity,
  avg(weather.clouds) as clouds,
  avg(weather.wind) as wind,
  avg(COALESCE(weather.rain,0)) AS current_rain,
  avg(COALESCE(roll.rain_24_hour_total,0)) as rain_24_hour,
  avg(COALESCE(weather.snow,0)) AS current_snow,
  avg(COALESCE(roll.snow_24_hour_total,0)) as snow_24_hour
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
  
  /* Remove covid-19 from the results */
WHERE foot.timestamp NOT BETWEEN '2020-01-01' AND '2022-12-31'
GROUP BY city, footfall_location, weekday, month, bank_holiday
