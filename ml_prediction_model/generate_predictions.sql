WITH
  last_90_days_footfall AS (
    /* Get the total daily footfall by location for the last 90 days */
  SELECT
    city,
    footfall_location,
    EXTRACT(DATE FROM `timestamp`) AS `date`,
    SUM(total_footfall) AS total_footfall
  FROM
    `nwilliams-msc-comp-sci.footfall.footfall_complete`
  WHERE
    EXTRACT(DATE FROM timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY
    city,
    footfall_location,
    `date` ),
  rolling_footfall AS (
    /* Calcuate the daily average for the last 90 days of foot traffic */
    SELECT city,
    footfall_location,
    ROUND(AVG(total_footfall), 0) AS rolling_footfall
FROM last_90_days_footfall
GROUP BY city, footfall_location

  ),
  rolling_weather AS (
    /* Get the previous daily rain and snow totals */
  SELECT
    fore.date,
    fore.city,
    fore.rain,
    fore.snow,
    COALESCE(LAG(fore.rain) OVER (PARTITION BY fore.city ORDER BY fore.date),SUM(hist.rain)) AS rain_24_hour,
    COALESCE(LAG(fore.snow) OVER (PARTITION BY fore.city ORDER BY fore.date),SUM(hist.snow)) AS snow_24_hour,
  FROM
    `nwilliams-msc-comp-sci.weather.forecasts` AS fore
  LEFT JOIN
    `nwilliams-msc-comp-sci.weather.historic_complete` AS hist
  ON
    DATE(hist.date) = DATE_SUB(fore.date, INTERVAL 1 DAY)
    AND fore.city = hist.city
  GROUP BY
    date,
    city,
    rain,
    snow
  ),
  model_input AS (
SELECT
  footfall.city,
  footfall.footfall_location,
  weather.date,
  FORMAT_DATE('%A', weather.date) AS weekday,
  FORMAT_DATE('%b', weather.date) AS month,
  CASE WHEN hols.name IS NOT NULL THEN 1 ELSE 0 END AS bank_holiday,
  footfall.rolling_footfall,
  ROUND((weather.min_temp + weather.max_temp) / 2,2) AS temp,
  weather.pressure,
  weather.humidity,
  weather.clouds,
  weather.wind,
  weather.rain AS current_rain,
  ROUND(rolling.rain_24_hour,2) AS rain_24_hour,
  weather.snow AS current_snow,
  rolling.snow_24_hour,
FROM
  rolling_footfall AS footfall
LEFT JOIN `weather.forecasts` AS weather
ON footfall.city = weather.city
LEFT JOIN rolling_weather as rolling
ON rolling.date = weather.date
AND rolling.city = weather.city
LEFT JOIN `holidays.holidays` AS hols
ON weather.date = hols.date
ORDER BY city, footfall_location, date
  )

SELECT
  * EXCEPT(predicted_total_footfall),
  ROUND(predicted_total_footfall) as predicted_footfall
FROM
  ML.PREDICT(MODEL `prediction_model.daily_footfall_predictor`,
    (
    SELECT
      *
    FROM model_input))
