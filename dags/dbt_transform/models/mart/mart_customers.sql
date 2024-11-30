{{ config(
   materialized="table"
) }}

SELECT
  CASE 
    WHEN Age >50 THEN 'Above 59'
    WHEN Age BETWEEN 50 AND 59 THEN '50-59'
    WHEN Age BETWEEN 40 AND 49 THEN '40-49'
    WHEN Age BETWEEN 30 AND 39 THEN '30-39'
    WHEN Age BETWEEN 20 AND 29 THEN '20-29'
    ELSE 'Below 20' 
  END AS Age_Range,
  Gender,
  Location,
  Subscription_status,
  COUNT(DISTINCT Customer_ID) Total_Customers
FROM {{ ref("dw_shopping_data") }}
GROUP BY 1,2,3,4