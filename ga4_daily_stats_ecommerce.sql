WITH session_start_times AS (
  SELECT
    event_name,
    event_date,
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_time,
    LEAD(TIMESTAMP_MICROS(event_timestamp)) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS next_session_start_time
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    event_name = 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201208'
), 

added_session_ids AS (
SELECT
  events.event_date,
  events.event_name,
  events.user_pseudo_id,
  TIMESTAMP_MICROS(events.event_timestamp) AS event_timestamp,
  session_start_times.session_start_time,
  SHA256(CONCAT(CAST(events.user_pseudo_id AS STRING), CAST(session_start_times.session_start_time AS STRING))) AS session_id
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS events
JOIN
  session_start_times AS session_start_times
ON
  events.user_pseudo_id = session_start_times.user_pseudo_id
  AND TIMESTAMP_MICROS(events.event_timestamp) < session_start_times.next_session_start_time
  AND TIMESTAMP_MICROS(events.event_timestamp) >= session_start_times.session_start_time
  AND events.event_name != 'session_start'
  AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201208'
), 

session_stats AS (

SELECT
  event_date, 
  session_id,
  user_pseudo_id,
  SUM(CASE WHEN event_name = 'user_engagement' THEN 1 ELSE 0 END) AS user_engagement,
  SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS page_views,  
  SUM(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS view_item,
  SUM(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart, 
  SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
  SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase, 
  TIMESTAMP_DIFF(MAX(event_timestamp), MAX(session_start_time), SECOND) AS session_duration
FROM 
  added_session_ids
GROUP BY 
  event_date,
  session_id, 
  user_pseudo_id

) 

SELECT 
  event_date, 
  COUNT(session_id) AS sessions, 
  COUNT(user_pseudo_id) AS users, 
  SUM(CASE WHEN user_engagement > 0 THEN 1 ELSE 0 END) AS engaged_sessions,
  FORMAT('%s%%', CAST(ROUND(SUM(CASE WHEN user_engagement > 0 THEN 0 ELSE 1 END)/COUNT(session_id)*100,2) AS STRING)) AS bounce_rate,
  TIME(TIMESTAMP_SECONDS(CAST(ROUND(AVG(session_duration), 0) AS INT64))) AS avg_session_duration, 
  SUM(page_views) AS page_views, 
  SUM(view_item) AS  view_items, 
  SUM(add_to_cart) AS add_to_cart, 
  SUM(begin_checkout) AS begin_checkout, 
  SUM(purchase) AS purchase, 
  FORMAT('%s%%', CAST(ROUND(SUM(CASE WHEN purchase > 0 THEN 1 ELSE 0 END) / COUNT(session_id)*100,2) AS STRING)) AS conversion_rate
FROM 
  session_stats
GROUP BY 
  event_date

  
