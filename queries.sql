--Расчет GMV по дням

SELECT
  ds,
  SUM(gmv) AS gmv_daily
FROM
  fact_purchases
WHERE
  istest = FALSE
GROUP BY
  ds
ORDER BY
  ds
 
-------------------------------------------------------------   
--Список активных покупателей и количество их покупок

SELECT
  user_id,
  COUNT(*) AS user_purchases
FROM
  fact_purchases
WHERE
  istest = FALSE
GROUP BY
  user_id
ORDER BY
  user_purchases DESC

-------------------------------------------------------------    
--Количество пользователей по стране регистрации

SELECT
  country,
  COUNT(*) AS country_users
FROM
  dim_users
WHERE
  ds = date_joined
  AND istest = FALSE
GROUP BY
  country
ORDER BY
  country_users DESC
 
-------------------------------------------------------------   
--Ключевое слово HAVING

SELECT
  ds,
  SUM(gmv) AS gmv_daily
FROM
  fact_purchases
WHERE
  istest = FALSE
GROUP BY
  ds
HAVING
  SUM(gmv) >= 1000
  AND SUM(gmv) <= 2000
ORDER BY
  ds
 
 
-------------------------------------------------------------  
 --DAU, WAU, MAU в разбивке по дням

SELECT
  ds,
  SUM(IF(is1dayactive = TRUE, 1, 0)) AS dau,
  SUM(IF(is7dayactive = TRUE, 1, 0)) AS wau,
  SUM(IF(is28dayactive = TRUE, 1, 0)) AS mau
FROM
  dim_users
WHERE
  istest = FALSE
GROUP BY
  ds
ORDER BY
  ds

-------------------------------------------------------------  
--Дневной Retention когорты пользователей
SELECT
  ds - date_joined AS days_from_joining,
  1.0 * SUM(IF(is1dayactive = TRUE, 1, 0)) / COUNT(*) AS d_retention,
  1.0 * SUM(IF(is7dayactive = TRUE, 1, 0)) / COUNT(*) AS w_retention,
  1.0 * SUM(IF(is28dayactive = TRUE, 1, 0)) / COUNT(*) AS m_retention,
  COUNT(*) AS cohort_total
FROM
  dim_users
WHERE
  date_joined >= '2019-02-01' 
  AND date_joined < '2019-03-01' 
  AND istest = FALSE
GROUP BY
  days_from_joining
ORDER BY
  days_from_joining
  
-------------------------------------------------------------  
--Зависимость дневного Retention от месяца регистрации

SELECT
  DATE_TRUNC('month', date_joined)::DATE as month_joined,
  ds - date_joined AS days_from_joining,
  1.0 * SUM(IF(is1dayactive = TRUE, 1, 0)) / COUNT(*) AS d_retention,
  1.0 * SUM(IF(is7dayactive = TRUE, 1, 0)) / COUNT(*) AS w_retention,
  1.0 * SUM(IF(is28dayactive = TRUE, 1, 0)) / COUNT(*) AS m_retention,
  COUNT(*) AS cohort_total
FROM
  dim_users
WHERE
  istest = FALSE
GROUP BY
  month_joined,
  days_from_joining
ORDER BY
  month_joined,
  days_from_joining
 
-------------------------------------------------------------   
--Зависимость 7-го дня Retention от месяца регистрации

SELECT
  s.month_joined,
  s.d_retention,
  s.w_retention,
  s.m_retention
FROM (
  SELECT
    DATE_TRUNC('month', date_joined)::DATE AS month_joined,
    ds - date_joined AS days_from_joining,
    1.0 * SUM(IF(is1dayactive = TRUE, 1, 0)) / COUNT(*) AS d_retention,
    1.0 * SUM(IF(is7dayactive = TRUE, 1, 0)) / COUNT(*) AS w_retention,
    1.0 * SUM(IF(is28dayactive = TRUE, 1, 0)) / COUNT(*) AS m_retention,
    COUNT(*) AS cohort_total
  FROM
    dim_users
  WHERE
    istest = FALSE
  GROUP BY
    month_joined,
    days_from_joining
  ) s
WHERE
  s.days_from_joining = 7
ORDER by s.month_joined


-------------------------------------------------------------  
--Траты на рекламные кампании и количество привлеченных пользователей

SELECT
  source,
  SUM(spent) AS channel_spent,
  SUM(new_users) AS total_users
FROM
  fact_paid_ua
WHERE
  ds >= '2019-02-01'
  AND ds < '2019-03-01'
GROUP BY
  source
ORDER BY
  source


-------------------------------------------------------------  
--Прибыль с привлеченных из разных каналов пользователей

SELECT
  SUM(revenue - 0.01 * gmv - 0.01 * revenue - psp_commission) AS gross_profit
FROM
  fact_purchases
WHERE
  istest = FALSE
  AND user_id IN (
    SELECT
      user_id
    FROM
      dim_users
    WHERE
      ds = '2019-07-07'
      AND istest = FALSE
      AND source IN ('paid_fb', 'paid_google', 'paid_other')
      AND date_joined >= '2019-02-01'
      AND date_joined < '2019-03-01'
  )


-------------------------------------------------------------  
--Расчет LTV когорты пользователей

WITH dgp AS (
  SELECT
    u.ds - u.date_joined AS dfj,
    u.source,
    SUM(p.revenue - 0.01 * p.gmv - 0.01 * p.revenue - p.psp_commission) / 
      COUNT(DISTINCT u.user_id) AS day_gp_per_user
  FROM
    dim_users AS u
  LEFT JOIN fact_purchases AS p 
  ON u.ds = p.ds AND u.user_id = p.user_id
  WHERE
    u.istest = FALSE
    AND u.date_joined >= '2019-02-01'
    AND u.date_joined < '2019-03-01'
  GROUP BY
    dfj,
    source
)
SELECT
  dgp.dfj,
  dgp.source,
  SUM(past_dgp.day_gp_per_user) AS ltv
FROM
  dgp
  JOIN dgp AS past_dgp 
  ON past_dgp.dfj <= dgp.dfj AND past_dgp.source = dgp.source
GROUP BY
  dgp.dfj,
  dgp.source
ORDER BY
  dgp.dfj,
  dgp.source
  
 
-------------------------------------------------------------   
  --Расчет активной аудитории скользящим окном
  WITH new_dim_users AS (
  SELECT
    ds,
    user_id,
    is1dayactive,
    BOOL_OR(is1dayactive) OVER (PARTITION BY user_id ORDER BY ds ROWS 6 PRECEDING) AS is7dayactive_calculated,
    BOOL_OR(is1dayactive) OVER (PARTITION BY user_id ORDER BY ds ROWS 27 PRECEDING) AS is28dayactive_calculated
  FROM
    dim_users
  WHERE
    istest = FALSE
)
SELECT
  ds,
  SUM(IF(is1dayactive, 1, 0)) AS dau,
  SUM(IF(is7dayactive_calculated, 1, 0)) AS wau,
  SUM(IF(is28dayactive_calculated, 1, 0)) AS mau
FROM
  new_dim_users
GROUP BY
  ds
ORDER BY
  ds
 
-------------------------------------------------------------   
 -- Прирост GMV

WITH month_gmv AS (
  SELECT
    DATE_TRUNC('month', ds)::DATE AS ds_m,
    SUM(gmv) AS gmv
  FROM
    fact_purchases
  WHERE
    istest = FALSE
  GROUP BY
    ds_m
)
SELECT
  ds_m,
  100.0 * (gmv - LAG(gmv) OVER (ORDER BY ds_m)) / (LAG(gmv) OVER (ORDER BY ds_m)) AS gmv_growth
FROM
  month_gmv
ORDER BY
  ds_m

-------------------------------------------------------------    
--Топ товаров по неделям

WITH products_gmv AS (
  SELECT
    DATE_TRUNC('week', ds)::DATE AS week,
    product_id,
    SUM(gmv) AS gmv
  FROM
    fact_purchases
  WHERE
    istest = FALSE
  GROUP BY
    week,
    product_id
),
products_with_gmvindex AS (
  SELECT
    week,
    product_id,
    gmv,
    ROW_NUMBER() OVER (PARTITION BY week ORDER BY gmv DESC) AS product_index
  FROM
    products_gmv
)
SELECT
  week,
  product_id,
  gmv,
  product_index
FROM
  products_with_gmvindex
WHERE
  product_index <= 10
ORDER BY
  week,
  gmv DESC
  
-------------------------------------------------------------  
-- LTV
WITH dgp AS (
  SELECT
    u.ds - u.date_joined AS dfj,
    u.source,
    SUM(p.revenue - 0.01 * p.gmv - 0.01 * p.revenue - p.psp_commission) / COUNT(DISTINCT u.user_id) AS day_gp_per_user
  FROM
    dim_users u
    LEFT JOIN fact_purchases p ON u.ds = p.ds
      AND u.user_id = p.user_id
  WHERE
    u.istest = FALSE
    AND u.date_joined >= '2019-02-01'
    AND u.date_joined < '2019-03-01'
  GROUP BY
    dfj,
    source
)
SELECT
  dfj,
  source,
  SUM(day_gp_per_user) OVER (PARTITION BY source ORDER BY dfj) AS ltv
FROM
  dgp
ORDER BY
  dfj,
  source 
  
  
-------------------------------------------------------------  
--Количество пользователей в когорте
SELECT
  COUNT(user_id) AS users_in_cohort
FROM
  dim_users
WHERE
  istest = FALSE
  AND date_joined >= '2019-02-01' -- начало когорты
  AND date_joined <= '2019-02-17' -- конец когорты
  AND ds = '2019-07-07'
  
  
-------------------------------------------------------------  
--Retention первых дней для когорты
SELECT
  ds - date_joined AS dfj,
  1.0 * SUM(IF(is1dayactive = TRUE, 1, 0)) / COUNT(*) AS d_retention
FROM
  dim_users
WHERE
  istest = FALSE AND 
  date_joined >= '2019-04-24' AND 
  date_joined <= '2019-05-11' AND 
  source != 'paid_other' AND 
  ds - date_joined <= 27 -- так как отсчет начинаем с 0
GROUP BY
  dfj
ORDER BY
  dfj
  
-------------------------------------------------------------  
--LTV первых дней для когорты
WITH dcm AS (
  SELECT
    u.ds - u.date_joined AS dfj,
    (SUM(p.revenue) - 0.01 * SUM(p.gmv) - 0.01 * SUM(p.revenue) - SUM(p.psp_commission)) / COUNT(DISTINCT u.user_id) AS day_cm_per_user
  FROM
    dim_users u
    LEFT JOIN fact_purchases p ON u.ds = p.ds
      AND u.user_id = p.user_id
  WHERE
    u.istest = FALSE
    AND u.date_joined >= '2019-02-01'
    AND u.date_joined <= '2019-02-17'
    AND u.ds - u.date_joined <= 27
  GROUP BY
    dfj
)
SELECT
  dcm.dfj,
  SUM(past_dcm.day_cm_per_user) AS ltv
FROM
  dcm
  JOIN dcm past_dcm ON past_dcm.dfj <= dcm.dfj
GROUP BY
  dcm.dfj
ORDER BY
  dcm.dfj

-------------------------------------------------------------  
--Доля платящих пользователей для когорты
SELECT
  1.0 * COUNT(DISTINCT fp.user_id) / COUNT(DISTINCT u.user_id) AS payer_share
FROM
  dim_users AS u
  LEFT JOIN fact_purchases fp ON fp.user_id = u.user_id
    AND fp.ds < u.date_joined + 28 -- аналогично можно было написать fp.ds <= u.date_joined + 27
WHERE
  u.ds = '2019-07-07'
  AND u.date_joined >= '2019-02-01'
  AND u.date_joined <= '2019-02-17'
  AND u.istest = FALSE
  
-------------------------------------------------------------  
--Кумулятивный GMV на пользователя для когорты
WITH dcm AS (
  SELECT
    u.ds - u.date_joined AS dfj,
    SUM(p.gmv) / COUNT(DISTINCT u.user_id) day_gmv_per_user
  FROM
    dim_users AS u
    LEFT JOIN fact_purchases p ON u.ds = p.ds
      AND u.user_id = p.user_id
  WHERE
    u.istest = FALSE
    AND u.date_joined >= '2019-02-01'
    AND u.date_joined <= '2019-02-17'
    AND u.ds - u.date_joined <= 27
  GROUP BY
    dfj
)
SELECT
  dcm.dfj,
  SUM(past_dcm.day_gmv_per_user) AS gmv_per_user
FROM
  dcm
  JOIN dcm past_dcm ON past_dcm.dfj <= dcm.dfj
GROUP BY
  dcm.dfj
ORDER BY
  dcm.dfj 
 
